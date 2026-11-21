package ninja.queries;

import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpData;
import ninja.Bucket;
import ninja.BrowserUploadResponder;
import ninja.PostPolicyVerifier;
import ninja.S3Policy;
import ninja.StoredObject;
import ninja.errors.S3ErrorCode;
import ninja.errors.S3ErrorSynthesizer;
import sirius.kernel.commons.Hasher;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.web.http.WebContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Handles presigned POST uploads.
 * <p>
 * A complete description of this protocol can be found here:
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-post-example.html">...</a>
 */
@Register(name = "presigned-post")
public class PresignedPostQueryProcessor implements S3QueryProcessor {

    /**
     * Names the variable which a key may contain, to be replaced with the name of the uploaded file.
     */
    private static final String FILENAME_VARIABLE = "${filename}";

    /**
     * Lists the form fields which describe the upload itself, and are therefore not stored along with the object.
     */
    private static final Set<String> FIELDS_NOT_STORED =
            Set.of("policy", "X-Amz-Signature", "X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "key", "file");

    @Part
    private S3ErrorSynthesizer errorSynthesizer;

    @Part
    private PostPolicyVerifier postPolicyVerifier;

    @Part
    private BrowserUploadResponder browserUploadResponder;

    @Override
    public void processQuery(@Nonnull WebContext webContext,
                             @Nonnull Bucket bucket,
                             @Nullable String key,
                             @Nonnull String query) {
        try {
            String formKey = extractKeyFromMultipartForm(webContext);
            HttpData filePart = webContext.getHttpData("file");
            String objectKey = replaceFilename(formKey, filePart);
            Optional<S3Policy> policy = verifyForm(webContext, bucket, objectKey);
            if (policy.isEmpty()) {
                return;
            }

            // Obtain file content: getContent() for raw body, or multipart "file" part when getContent() is null
            InputStream contentStream = webContext.getContent() != null ? webContext.getContent() : openFile(filePart);
            if (contentStream == null && (filePart == null || filePart.getByteBuf() == null)) {
                errorSynthesizer.synthesiseError(webContext,
                                                 bucket.getName(),
                                                 objectKey,
                                                 S3ErrorCode.InvalidRequest,
                                                 "Missing file content in form data");
                return;
            }

            long contentSize = filePart != null ? filePart.length() : webContext.getContentSize();
            S3ErrorCode sizeError = postPolicyVerifier.verifySize(policy.get(), contentSize);
            if (sizeError != null) {
                errorSynthesizer.synthesiseError(webContext,
                                                 bucket.getName(),
                                                 objectKey,
                                                 sizeError,
                                                 PostPolicyVerifier.ERROR_SIZE_NOT_PERMITTED);
                return;
            }

            // Create bucket if it doesn't exist
            if (!bucket.exists() && !bucket.create()) {
                errorSynthesizer.synthesiseError(webContext,
                                                 bucket.getName(),
                                                 objectKey,
                                                 S3ErrorCode.InternalError,
                                                 "Failed to create bucket");
                return;
            }

            StoredObject object = bucket.getObject(objectKey);
            writeContent(object, contentStream, filePart);
            Map<String, String> properties = collectProperties(webContext, object);
            object.setProperties(properties);

            browserUploadResponder.respond(webContext, bucket, objectKey, properties.get("ETag"));
        } catch (IOException exception) {
            errorSynthesizer.synthesiseError(webContext,
                                             bucket.getName(),
                                             key,
                                             S3ErrorCode.InternalError,
                                             "Internal error processing POST upload: " + Exceptions.handle(exception)
                                                                                                   .getMessage());
        }
    }

    /**
     * Verifies the key and the policy of the form, and answers the request with an error if they are not in order.
     * <p>
     * As for S3, the conditions of the policy are evaluated for the key with the name of the uploaded file filled in.
     * A condition for a key named as <tt>uploads/${filename}</tt> therefore has to be
     * <tt>["starts-with", "$key", "uploads/"]</tt>.
     *
     * @param webContext the request carrying the form
     * @param bucket     the bucket to upload into
     * @param objectKey  the key to store the object under, i.e. the key of the form with the name of the uploaded
     *                   file filled in
     * @return the policy of the form, or an empty optional if the request has been answered with an error
     */
    private Optional<S3Policy> verifyForm(WebContext webContext, Bucket bucket, @Nullable String objectKey) {
        if (Strings.isEmpty(objectKey)) {
            errorSynthesizer.synthesiseError(webContext,
                                             bucket.getName(),
                                             null,
                                             S3ErrorCode.InvalidRequest,
                                             "Missing key in form data");
            return Optional.empty();
        }
        if (!StoredObject.isStorableKey(objectKey)) {
            errorSynthesizer.synthesiseError(webContext,
                                             bucket.getName(),
                                             objectKey,
                                             S3ErrorCode.KeyTooLongError,
                                             "Your key is too long");
            return Optional.empty();
        }

        // Nothing but its policy authorizes this request, as it has been let through without a signature of its own.
        PostPolicyVerifier.Result verification = postPolicyVerifier.verify(webContext, bucket, objectKey);
        if (!verification.isValid()) {
            errorSynthesizer.synthesiseError(webContext,
                                             bucket.getName(),
                                             objectKey,
                                             verification.errorCode(),
                                             verification.message());
            return Optional.empty();
        }
        return Optional.of(verification.policy());
    }

    /**
     * Fills in the name of the uploaded file where the key of the form contains <tt>${filename}</tt>.
     * <p>
     * As for S3, the variable is replaced with an empty string if the form names no file.
     *
     * @param formKey  the key as stated by the form, if any
     * @param filePart the uploaded part of the form, if any
     * @return the key to store the object under
     */
    @Nullable
    private String replaceFilename(@Nullable String formKey, @Nullable HttpData filePart) {
        if (formKey == null || !formKey.contains(FILENAME_VARIABLE)) {
            return formKey;
        }
        String filename = filePart instanceof FileUpload upload ? upload.getFilename() : null;
        return formKey.replace(FILENAME_VARIABLE, filename == null ? "" : filename);
    }

    /**
     * Opens the file in which the uploaded part of the form has been stored.
     *
     * @param filePart the uploaded part of the form, if any
     * @return the content of the part, or <tt>null</tt> if there is no part or it is kept in memory instead of a file
     */
    @Nullable
    private InputStream openFile(@Nullable HttpData filePart) {
        if (filePart == null) {
            return null;
        }
        try {
            File partFile = filePart.getFile();
            if (partFile != null && partFile.exists()) {
                return new FileInputStream(partFile);
            }
        } catch (IOException _) {
            // Data is not stored in a file, will use getByteBuf() later
        }
        return null;
    }

    /**
     * Writes the uploaded content into the file of the object.
     *
     * @param object        the object to write
     * @param contentStream the content as stream, or <tt>null</tt> to take it from the buffer of the part
     * @param filePart      the uploaded part of the form, whose buffer holds the content if there is no stream
     * @throws IOException if the content cannot be written
     */
    private void writeContent(StoredObject object, @Nullable InputStream contentStream, HttpData filePart)
            throws IOException {
        try (FileOutputStream out = new FileOutputStream(object.getFile())) {
            if (contentStream != null) {
                ByteStreams.copy(contentStream, out);
            } else {
                ByteBuf buf = filePart.getByteBuf();
                if (buf != null && buf.readableBytes() > 0) {
                    buf.getBytes(buf.readerIndex(), out, buf.readableBytes());
                }
            }
        } finally {
            if (contentStream instanceof FileInputStream) {
                closeQuietly(contentStream);
            }
        }
    }

    private void closeQuietly(InputStream stream) {
        try {
            stream.close();
        } catch (IOException exception) {
            // Closing the file of an uploaded part cannot fail in a way which matters once its content is stored.
            Exceptions.ignore(exception);
        }
    }

    /**
     * Collects the properties to store along with the uploaded object: its entity tag, its content type and the
     * further fields of the form.
     *
     * @param webContext the request carrying the form
     * @param object     the object which has been written
     * @return the properties of the object
     */
    private Map<String, String> collectProperties(WebContext webContext, StoredObject object) {
        byte[] hash = Hasher.md5().hashFile(object.getFile()).toHash();
        String etag = BaseEncoding.base16().encode(hash).toLowerCase();

        Map<String, String> properties = Maps.newHashMap();
        properties.put("ETag", etag);

        // Set content type if provided
        String contentType = PostPolicyVerifier.readFormField(webContext, "Content-Type").asString();
        if (Strings.isFilled(contentType)) {
            properties.put("Content-Type", contentType);
        }

        // Store additional form parameters as metadata (excluding AWS and internal parameters, and fields after the
        // file, which S3 ignores)
        Set<String> fieldsAfterFile = PostPolicyVerifier.determineFieldsAfterFile(webContext);
        for (String paramName : webContext.getParameterNames()) {
            // Checked first, so that the uploaded file is never read as the value of a field.
            if (!FIELDS_NOT_STORED.contains(paramName) && !fieldsAfterFile.contains(paramName)) {
                String value = webContext.get(paramName).asString();
                if (Strings.isFilled(value)) {
                    properties.put(paramName, value);
                }
            }
        }
        return properties;
    }

    /**
     * Extracts the key from a multipart form request
     *
     * @param webContext the request context
     * @return the extracted key or null if not found
     */
    private String extractKeyFromMultipartForm(WebContext webContext) {
        // Works for both form fields and parameters of the query.
        String key = PostPolicyVerifier.readFormField(webContext, "key").getString();
        return Strings.isFilled(key) ? key : null;
    }
}

