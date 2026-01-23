/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja.queries;

import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.multipart.FileUpload;
import ninja.Bucket;
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Handles presigned POST uploads.
 * <p>
 * A complete description of this protocol can be found here:
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-post-example.html">...</a>
 */
@Register(name = "presigned-post")
public class PresignedPostQueryProcessor implements S3QueryProcessor {

    @Part
    private S3ErrorSynthesizer errorSynthesizer;

    @Override
    public void processQuery(@Nonnull WebContext webContext,
                             @Nonnull Bucket bucket,
                             @Nullable String key,
                             @Nonnull String query) {
        try {
            // Extract key from form data
            String objectKey = extractKeyFromMultipartForm(webContext);
            if (Strings.isEmpty(objectKey)) {
                errorSynthesizer.synthesiseError(webContext,
                                                 bucket.getName(),
                                                 null,
                                                 S3ErrorCode.InvalidRequest,
                                                 "Missing key in form data");
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

            // Create object and store content
            StoredObject object = bucket.getObject(objectKey);

            try (FileOutputStream out = new FileOutputStream(object.getFile())) {
                Object fileObj = webContext.get("file").get();
                if (fileObj instanceof FileUpload fileUpload) {
                    byte[] content = fileUpload.get();
                    out.write(content);
                }
                // If no content was written, create an empty file (valid for S3)
            }

            // Calculate MD5 hash and ETag
            byte[] hash = Hasher.md5().hashFile(object.getFile()).toHash();
            String etag = BaseEncoding.base16().encode(hash).toLowerCase();

            // Collect properties from form data
            Map<String, String> properties = Maps.newHashMap();
            properties.put("ETag", etag);

            // Set content type if provided
            String contentType = webContext.get("Content-Type").asString();
            if (Strings.isFilled(contentType)) {
                properties.put("Content-Type", contentType);
            }

            // Store additional form parameters as metadata (excluding AWS and internal parameters)
            for (String paramName : webContext.getParameterNames()) {
                if (!"policy".equals(paramName)
                    && !"X-Amz-Signature".equals(paramName)
                    && !"X-Amz-Algorithm".equals(paramName)
                    && !"X-Amz-Credential".equals(paramName)
                    && !"X-Amz-Date".equals(paramName)
                    && !"key".equals(paramName)
                    && !"file".equals(paramName)) {
                    String value = webContext.get(paramName).asString();
                    if (Strings.isFilled(value)) {
                        properties.put(paramName, value);
                    }
                }
            }

            object.setProperties(properties);

            // Return success response
            String successActionStatus = webContext.get("success_action_status").asString("204");
            HttpResponseStatus status;

            switch (successActionStatus) {
                case "200":
                    status = HttpResponseStatus.OK;
                    break;
                case "201":
                    status = HttpResponseStatus.CREATED;
                    break;
                case "204":
                    status = HttpResponseStatus.NO_CONTENT;
                    break;
                default:
                    // Invalid success_action_status - AWS S3 would reject this
                    errorSynthesizer.synthesiseError(webContext,
                                                     bucket.getName(),
                                                     objectKey,
                                                     S3ErrorCode.InvalidRequest,
                                                     "Invalid success_action_status: "
                                                     + successActionStatus
                                                     + ". Must be 200, 201, or 204.");
                    return;
            }

            webContext.respondWith().status(status);
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
     * Extracts the key from a multipart form request
     *
     * @param webContext the request context
     * @return the extracted key or null if not found
     */
    private String extractKeyFromMultipartForm(WebContext webContext) {
        // First try the standard parameter (works for both form fields and query params)
        String key = webContext.getParameter("key");
        if (Strings.isFilled(key)) {
            return key;
        }

        // Try to get it from request parameter (different method for multipart)
        if (webContext.hasParameter("key")) {
            key = webContext.get("key").asString();
            if (Strings.isFilled(key)) {
                return key;
            }
        }

        return null;
    }
}
