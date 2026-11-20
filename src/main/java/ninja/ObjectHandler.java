/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import ninja.errors.S3ErrorCode;
import sirius.kernel.cache.Cache;
import sirius.kernel.commons.Hasher;
import sirius.kernel.commons.Outcall;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Urls;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.xml.XMLStructuredOutput;
import sirius.web.http.InputStreamHandler;
import sirius.web.http.MimeHelper;
import sirius.web.http.Response;
import sirius.web.http.WebContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Handles the S3 API calls which deal with single objects: storing, reading, copying and deleting them.
 */
@Register(classes = ObjectHandler.class)
public class ObjectHandler {

    private static final String PATH_DELIMITER = "/";

    /**
     * Matches a range which states its first byte, so that a range starting behind the end of an object is noticed.
     */
    private static final Pattern OPEN_RANGE_PATTERN = Pattern.compile("bytes=(\\d{1,18})-(\\d{0,18})");

    /**
     * Ends the names of the files into which objects are received within the folder of their bucket.
     * <p>
     * Such a file starts with "$", which no file of an object does, as "$" is encoded in object keys.
     */
    static final String RECEIVING_FILE_SUFFIX = ".receiving";

    /**
     * Starts the version which S3 permits at the end of an <tt>x-amz-copy-source</tt> header.
     */
    private static final String COPY_SOURCE_VERSION_SUFFIX = "?versionId=";

    private static final Map<String, String> headerOverrides;

    static {
        headerOverrides = Maps.newTreeMap();
        headerOverrides.put("response-content-type", S3Dispatcher.HTTP_HEADER_NAME_CONTENT_TYPE);
        headerOverrides.put("response-content-language", "Content-Language");
        headerOverrides.put("response-expires", "Expires");
        headerOverrides.put("response-cache-control", "Cache-Control");
        headerOverrides.put("response-content-disposition", "Content-Disposition");
        headerOverrides.put("response-content-encoding", "Content-Encoding");
    }

    @Part
    private Storage storage;

    @Part
    private S3Responder responder;

    @Part
    private RequestAuthenticator authenticator;

    @Part
    private PayloadVerifier payloadVerifier;

    @Part
    private Preconditions preconditions;

    /**
     * Handles DELETE /bucket/id
     *
     * @param webContext the context describing the current request
     * @param bucket     the bucket containing the object to delete
     * @param id         name of the object to delete
     */
    void deleteObject(final WebContext webContext, final Bucket bucket, final String id) {
        StoredObject object = bucket.getObject(id);
        object.delete();

        webContext.respondWith().status(HttpResponseStatus.NO_CONTENT);
        responder.signalObjectSuccess(webContext);
    }

    /**
     * Handles PUT /bucket/id
     *
     * @param webContext the context describing the current request
     * @param bucket     the bucket containing the object to upload
     * @param id         name of the object to upload
     */
    void putObject(WebContext webContext, Bucket bucket, String id, InputStreamHandler inputStream) throws IOException {
        StoredObject object = bucket.getObject(id);
        if (inputStream == null) {
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        id,
                                        S3ErrorCode.IncompleteBody,
                                        "No content posted");
            return;
        }
        if (preconditions.evaluateWrite(webContext, object) == Preconditions.Outcome.FAILED) {
            ByteStreams.exhaust(inputStream);
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        id,
                                        S3ErrorCode.PreconditionFailed,
                                        S3Dispatcher.ERROR_PRECONDITION_FAILED);
            return;
        }

        // Received next to the object, as only a file on the same file system can replace it in one step. A request
        // which is refused therefore leaves the object as it was. Files starting with "$" are no objects.
        File receivedFile = new File(bucket.getFolder(), "$" + UUID.randomUUID() + RECEIVING_FILE_SUFFIX);
        try {
            try (FileOutputStream out = new FileOutputStream(receivedFile)) {
                ByteStreams.copy(inputStream, out);
            }

            Optional<Tuple<S3ErrorCode, String>> payloadError = payloadVerifier.verify(webContext, receivedFile);
            if (payloadError.isPresent()) {
                responder.signalObjectError(webContext,
                                            bucket.getName(),
                                            id,
                                            payloadError.get().getFirst(),
                                            payloadError.get().getSecond());
                return;
            }

            String etag = Hasher.md5().hashFile(receivedFile).toHexString();
            Map<String, String> properties = StoredHeaders.ofRequest(webContext);
            properties.put(S3Dispatcher.HTTP_HEADER_NAME_ETAG, etag);
            object.replaceWith(receivedFile, properties);

            Response response = webContext.respondWith();
            response.addHeader(S3Dispatcher.HTTP_HEADER_NAME_ETAG, quoteEtag(etag)).status(HttpResponseStatus.OK);
            response.addHeader(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS, S3Dispatcher.HTTP_HEADER_NAME_ETAG);
            responder.signalObjectSuccess(webContext);
        } finally {
            // Gone if the object has been written. Whatever else happened, the file must not stay behind.
            sirius.kernel.commons.Files.delete(receivedFile);
        }
    }

    static String quoteEtag(String etag) {
        return "\"" + etag + "\"";
    }

    /**
     * Handles GET /bucket/id with an <tt>x-amz-copy-source</tt> header.
     *
     * @param webContext the context describing the current request
     * @param bucket     the bucket containing the object to use as destination
     * @param key        the key of the object to use as destination
     * @param sourcePath the path of the source object to copy from
     */
    void copyObject(WebContext webContext, Bucket bucket, String key, String sourcePath) throws IOException {
        Optional<StoredObject> source = resolveCopySource(webContext, sourcePath);
        if (source.isEmpty()) {
            return;
        }
        StoredObject sourceObject = source.get();

        StoredObject object = bucket.getObject(key);
        String metadataDirective = webContext.getHeader("x-amz-metadata-directive");
        if (Strings.isFilled(metadataDirective) && !"COPY".equals(metadataDirective) && !"REPLACE".equals(
                metadataDirective)) {
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        key,
                                        S3ErrorCode.InvalidArgument,
                                        "Unknown metadata directive.");
            return;
        }

        boolean replaceMetadata = "REPLACE".equals(metadataDirective);
        if (!replaceMetadata && sourceObject.getFile().equals(object.getFile())) {
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        key,
                                        S3ErrorCode.InvalidRequest,
                                        "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata.");
            return;
        }

        // A copy is a plain object, even if its source has been uploaded in parts and thus carries another entity tag.
        String etag = Hasher.md5().hashFile(sourceObject.getFile()).toHexString();
        Map<String, String> properties =
                replaceMetadata ? StoredHeaders.ofRequest(webContext) : sourceObject.getProperties();
        properties.put(S3Dispatcher.HTTP_HEADER_NAME_ETAG, etag);

        File copiedFile = new File(bucket.getFolder(), "$" + UUID.randomUUID() + RECEIVING_FILE_SUFFIX);
        try {
            Files.copy(sourceObject.getFile(), copiedFile);
            object.replaceWith(copiedFile, properties);
        } finally {
            sirius.kernel.commons.Files.delete(copiedFile);
        }

        XMLStructuredOutput structuredOutput =
                webContext.respondWith().addHeader(S3Dispatcher.HTTP_HEADER_NAME_ETAG, quoteEtag(etag)).xml();
        structuredOutput.beginOutput("CopyObjectResult");
        structuredOutput.beginObject("LastModified");
        structuredOutput.text(S3Dispatcher.ISO8601_INSTANT.format(object.getLastModifiedInstant()));
        structuredOutput.endObject();
        structuredOutput.beginObject(S3Dispatcher.HTTP_HEADER_NAME_ETAG);
        structuredOutput.text(quoteEtag(etag));
        structuredOutput.endObject();
        structuredOutput.endOutput();
        responder.signalObjectSuccess(webContext);
    }

    /**
     * Resolves the object named by an <tt>x-amz-copy-source</tt> header, and answers the request with an error if
     * there is none.
     *
     * @param webContext the context describing the current request
     * @param sourcePath the path of the source object, as <tt>bucket/key</tt> with an optional leading slash. The
     *                   path may be URL-encoded as a whole, and may end with <tt>?versionId=</tt>, which is ignored
     *                   as there are no versions.
     * @return the object to copy from, or an empty optional if the request has been answered with an error
     */
    Optional<StoredObject> resolveCopySource(WebContext webContext, String sourcePath) {
        // The version is cut off before decoding, as a question mark which belongs to the key is sent encoded.
        String path = Strings.isEmpty(sourcePath) ? "" : sourcePath;
        int versionStart = path.indexOf(COPY_SOURCE_VERSION_SUFFIX);
        if (versionStart >= 0) {
            path = path.substring(0, versionStart);
        }

        // Decoded before it is split, as clients are free to encode the slash after the bucket as well.
        path = Urls.decode(path);
        if (path.startsWith(PATH_DELIMITER)) {
            path = path.substring(PATH_DELIMITER.length());
        }
        int keyStart = path.indexOf(PATH_DELIMITER);
        if (keyStart <= 0 || keyStart == path.length() - 1) {
            responder.signalObjectError(webContext,
                                        null,
                                        null,
                                        S3ErrorCode.InvalidRequest,
                                        String.format("Source '%s' must name a bucket and a key, separated by '/'",
                                                      sourcePath));
            return Optional.empty();
        }

        String sourceBucketName = path.substring(0, keyStart);
        String sourceKey = path.substring(keyStart + 1);

        if (!Bucket.isValidName(sourceBucketName)) {
            responder.signalObjectError(webContext,
                                        sourceBucketName,
                                        sourceKey,
                                        S3ErrorCode.NoSuchBucket,
                                        String.format("Source bucket '%s' does not exist", sourceBucketName));
            return Optional.empty();
        }

        Bucket sourceBucket = storage.getBucket(sourceBucketName);
        if (!sourceBucket.exists()) {
            responder.signalObjectError(webContext,
                                        sourceBucketName,
                                        sourceKey,
                                        S3ErrorCode.NoSuchBucket,
                                        String.format("Source bucket '%s' does not exist", sourceBucketName));
            return Optional.empty();
        }

        // The request has only been checked against its target so far. Without this, anyone permitted to write
        // somewhere could copy objects out of a private bucket.
        if (!authenticator.objectCheckAuth(webContext, sourceBucket, sourceKey)) {
            return Optional.empty();
        }

        // Only a file is an object. A key such as ".." names a folder, which exists, but cannot be copied.
        StoredObject sourceObject = sourceBucket.getObject(sourceKey);
        if (!sourceObject.getFile().isFile()) {
            responder.signalObjectError(webContext,
                                        sourceBucketName,
                                        sourceKey,
                                        S3ErrorCode.NoSuchKey,
                                        String.format("Source object '%s/%s' does not exist",
                                                      sourceBucketName,
                                                      sourceKey));
            return Optional.empty();
        }

        if (preconditions.evaluateCopySource(webContext, sourceObject) == Preconditions.Outcome.FAILED) {
            responder.signalObjectError(webContext,
                                        sourceBucketName,
                                        sourceKey,
                                        S3ErrorCode.PreconditionFailed,
                                        S3Dispatcher.ERROR_PRECONDITION_FAILED);
            return Optional.empty();
        }

        return Optional.of(sourceObject);
    }

    /**
     * Handles GET /bucket/id
     *
     * @param webContext the context describing the current request
     * @param bucket     the bucket containing the object to download
     * @param id         name of the object to use as download
     */
    void getObject(WebContext webContext, Bucket bucket, String id, boolean sendFile) throws IOException {
        StoredObject object = bucket.getObject(id);

        if (!object.exists()) {
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        id,
                                        S3ErrorCode.NoSuchKey,
                                        "Object does not exist");
            return;
        }

        Matcher rangeMatcher =
                OPEN_RANGE_PATTERN.matcher(webContext.getHeaderValue(HttpHeaderNames.RANGE).asString("").trim());
        if (rangeMatcher.matches()) {
            if (Long.parseLong(rangeMatcher.group(1)) >= object.getSizeBytes()) {
                responder.signalObjectError(webContext,
                                            bucket.getName(),
                                            id,
                                            S3ErrorCode.InvalidRange,
                                            "The requested range is not satisfiable");
                return;
            }
            if (Strings.isFilled(rangeMatcher.group(2))
                && Long.parseLong(rangeMatcher.group(2)) >= object.getSizeBytes()) {
                // S3 serves what is there of a range which reaches beyond the end, whereas sending the file would
                // refuse it. The range is therefore cut to the size of the object.
                webContext.getRequest()
                          .headers()
                          .set(HttpHeaderNames.RANGE,
                               "bytes=" + rangeMatcher.group(1) + "-" + (object.getSizeBytes() - 1));
            }
        }

        Preconditions.Outcome outcome = preconditions.evaluateRead(webContext, object);
        if (outcome == Preconditions.Outcome.FAILED) {
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        id,
                                        S3ErrorCode.PreconditionFailed,
                                        S3Dispatcher.ERROR_PRECONDITION_FAILED);
            return;
        }
        if (outcome == Preconditions.Outcome.NOT_MODIFIED) {
            webContext.respondWith().status(HttpResponseStatus.NOT_MODIFIED);
            responder.signalObjectSuccess(webContext);
            return;
        }

        Response response = webContext.respondWith();
        Map<String, String> properties = object.getProperties();
        // A checksum covers the whole object, so it must not accompany a part of it.
        boolean partialContent = Strings.isFilled(webContext.getHeader(HttpHeaderNames.RANGE));
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (StoredHeaders.isStored(entry.getKey()) && !(partialContent
                                                            && StoredHeaders.isChecksum(entry.getKey()))) {
                response.setHeader(entry.getKey(), entry.getValue());
            }
        }
        for (Map.Entry<String, String> entry : getOverridenHeaders(webContext).entrySet()) {
            response.setHeader(entry.getKey(), entry.getValue());
        }

        // check for the ETAG, set it if necessary, and add it to the response
        String etag = properties.get(S3Dispatcher.HTTP_HEADER_NAME_ETAG);
        if (Strings.isEmpty(etag)) {
            etag = BaseEncoding.base16().encode(Hasher.md5().hashFile(object.getFile()).toHash()).toLowerCase();
            properties.put(S3Dispatcher.HTTP_HEADER_NAME_ETAG, etag);
            object.setProperties(properties);
        }
        response.setHeader(S3Dispatcher.HTTP_HEADER_NAME_ETAG, quoteEtag(etag.toLowerCase()));
        response.addHeader(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS, S3Dispatcher.HTTP_HEADER_NAME_ETAG);

        // Set explicitly, as sending the file skips this header once the object brings its own caching headers.
        response.setHeader(HttpHeaderNames.LAST_MODIFIED,
                           Outcall.RFC2616_INSTANT.format(Instant.ofEpochMilli(object.getFile().lastModified())));

        if (sendFile) {
            response.file(object.getFile());
        } else {
            // The content type is only guessed if none has been stored along with the object.
            String contentType = MimeHelper.guessMimeType(object.getFile().getName());
            response.addHeaderIfNotExists(HttpHeaderNames.CONTENT_TYPE, contentType);
            response.addHeader(HttpHeaderNames.CONTENT_LENGTH, object.getFile().length());
            response.status(HttpResponseStatus.OK);
        }
        responder.signalObjectSuccess(webContext);
    }

    private Map<String, String> getOverridenHeaders(WebContext webContext) {
        Map<String, String> overrides = Maps.newTreeMap();
        for (Map.Entry<String, String> entry : headerOverrides.entrySet()) {
            String header = entry.getValue();
            String value = webContext.getParameter(entry.getKey());
            if (value != null) {
                overrides.put(header, value);
            }
        }
        return overrides;
    }
}
