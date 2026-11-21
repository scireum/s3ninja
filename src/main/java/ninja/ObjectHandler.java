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
import java.util.concurrent.locks.Lock;
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
     * Matches a range which asks for the given number of bytes at the end of an object.
     */
    private static final Pattern SUFFIX_RANGE_PATTERN = Pattern.compile("bytes=-(\\d{1,18})");

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

    @Part
    private ObjectLocks objectLocks;

    /**
     * Handles DELETE /bucket/id
     *
     * @param webContext the context describing the current request
     * @param bucket     the bucket containing the object to delete
     * @param id         name of the object to delete
     */
    void deleteObject(final WebContext webContext, final Bucket bucket, final String id) {
        StoredObject object = bucket.getObject(id);
        Lock lock = objectLocks.lockFor(object);
        lock.lock();
        try {
            object.delete();
        } finally {
            lock.unlock();
        }

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
        Optional<String> etag = storeObject(webContext, bucket, id, inputStream);
        if (etag.isPresent()) {
            Response response = webContext.respondWith();
            response.addHeader(S3Dispatcher.HTTP_HEADER_NAME_ETAG, quoteEtag(etag.get())).status(HttpResponseStatus.OK);
            response.addHeader(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS, S3Dispatcher.HTTP_HEADER_NAME_ETAG);
            responder.signalObjectSuccess(webContext);
        }
    }

    /**
     * Stores the content of a request as the given object, and answers the request with an error if it is refused.
     * <p>
     * The content is verified against what the request states about it, and the conditions of the request are
     * evaluated, before the object is replaced. Answering a successful request is left to the caller, as PutObject and
     * browser-based uploads answer differently.
     *
     * @param webContext  the request to store the content of
     * @param bucket      the bucket containing the object
     * @param id          the key of the object
     * @param inputStream the content of the object
     * @return the entity tag of the stored object, without quotes, or an empty optional if the request has been
     * answered with an error
     * @throws IOException if the content cannot be received or stored
     */
    Optional<String> storeObject(WebContext webContext, Bucket bucket, String id, InputStreamHandler inputStream)
            throws IOException {
        StoredObject object = bucket.getObject(id);
        if (inputStream == null) {
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        id,
                                        S3ErrorCode.IncompleteBody,
                                        "No content posted");
            return Optional.empty();
        }
        if (preconditions.evaluateWrite(webContext, object) == Preconditions.Outcome.FAILED) {
            ByteStreams.exhaust(inputStream);
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        id,
                                        S3ErrorCode.PreconditionFailed,
                                        S3Dispatcher.ERROR_PRECONDITION_FAILED);
            return Optional.empty();
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
                return Optional.empty();
            }

            String etag = Hasher.md5().hashFile(receivedFile).toHexString();
            Map<String, String> properties = StoredHeaders.ofRequest(webContext);
            properties.put(S3Dispatcher.HTTP_HEADER_NAME_ETAG, etag);
            if (!replaceIfPermitted(webContext, object, receivedFile, properties)) {
                responder.signalObjectError(webContext,
                                            bucket.getName(),
                                            id,
                                            S3ErrorCode.PreconditionFailed,
                                            S3Dispatcher.ERROR_PRECONDITION_FAILED);
                return Optional.empty();
            }
            return Optional.of(etag);
        } finally {
            // Gone if the object has been written. Whatever else happened, the file must not stay behind.
            sirius.kernel.commons.Files.delete(receivedFile);
        }
    }

    /**
     * Makes the given file and properties the new state of the object, unless the conditions of the request forbid
     * it.
     * <p>
     * The conditions are evaluated and the object is replaced while holding the lock of the object. Of two requests
     * which only want to create an object, exactly one therefore succeeds.
     *
     * @param webContext the request which writes the object
     * @param object     the object to replace or to create
     * @param content    the file holding the new content, which has to reside in the folder of the bucket
     * @param properties the properties of the new object
     * @return <tt>true</tt> if the object has been written, <tt>false</tt> if a condition of the request did not hold
     * @throws IOException if the object cannot be written, in which case its previous state is kept
     */
    boolean replaceIfPermitted(WebContext webContext, StoredObject object, File content, Map<String, String> properties)
            throws IOException {
        Lock lock = objectLocks.lockFor(object);
        lock.lock();
        try {
            if (preconditions.evaluateWrite(webContext, object) == Preconditions.Outcome.FAILED) {
                return false;
            }
            object.replaceWith(content, properties);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reads what a copy needs of its source.
     *
     * @param <T> the type of what is read
     */
    @FunctionalInterface
    interface CopySourceReader<T> {

        /**
         * Reads the given source.
         *
         * @param source the object to copy from, which cannot change while it is read
         * @return what has been read, which must not be <tt>null</tt>
         * @throws IOException if the source cannot be read
         */
        T read(StoredObject source) throws IOException;
    }

    /**
     * Reads the source of a copy while holding its lock, after verifying that it still is what the request asks for.
     * <p>
     * The conditions a request states for its source have been evaluated when the source was resolved. Until it is
     * read, another request may replace it. The source is therefore read while holding its lock, after its conditions
     * have been evaluated once more. The lock is held for the reading only.
     *
     * @param webContext the request which copies the object
     * @param source     the object to copy from
     * @param reader     reads what the copy needs of the source
     * @param <T>        the type of what is read
     * @return what has been read, or an empty optional if the request has been answered with an error as the source
     * no longer exists or no longer meets its conditions
     * @throws IOException if the source cannot be read
     */
    <T> Optional<T> readCopySource(WebContext webContext, StoredObject source, CopySourceReader<T> reader)
            throws IOException {
        Lock lock = objectLocks.lockFor(source);
        lock.lock();
        try {
            if (!source.getFile().isFile()) {
                responder.signalObjectError(webContext,
                                            null,
                                            source.getKey(),
                                            S3ErrorCode.NoSuchKey,
                                            "The source object does not exist");
                return Optional.empty();
            }
            if (preconditions.evaluateCopySource(webContext, source) == Preconditions.Outcome.FAILED) {
                responder.signalObjectError(webContext,
                                            null,
                                            source.getKey(),
                                            S3ErrorCode.PreconditionFailed,
                                            S3Dispatcher.ERROR_PRECONDITION_FAILED);
                return Optional.empty();
            }
            return Optional.of(reader.read(source));
        } finally {
            lock.unlock();
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

        String etag;
        File copiedFile = new File(bucket.getFolder(), "$" + UUID.randomUUID() + RECEIVING_FILE_SUFFIX);
        try {
            Optional<Map<String, String>> copiedProperties = readCopySource(webContext, sourceObject, lockedSource -> {
                // A copy is a plain object, even if its source has been uploaded in parts and thus carries another
                // entity tag.
                Map<String, String> properties =
                        replaceMetadata ? StoredHeaders.ofRequest(webContext) : lockedSource.getProperties();
                properties.put(S3Dispatcher.HTTP_HEADER_NAME_ETAG,
                               Hasher.md5().hashFile(lockedSource.getFile()).toHexString());
                Files.copy(lockedSource.getFile(), copiedFile);
                return properties;
            });
            if (copiedProperties.isEmpty()) {
                return;
            }
            Map<String, String> properties = copiedProperties.get();
            etag = properties.get(S3Dispatcher.HTTP_HEADER_NAME_ETAG);

            if (!replaceIfPermitted(webContext, object, copiedFile, properties)) {
                responder.signalObjectError(webContext,
                                            bucket.getName(),
                                            key,
                                            S3ErrorCode.PreconditionFailed,
                                            S3Dispatcher.ERROR_PRECONDITION_FAILED);
                return;
            }
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
            Response notModified = webContext.respondWith();
            String storedEtag = object.getProperties().get(S3Dispatcher.HTTP_HEADER_NAME_ETAG);
            if (Strings.isFilled(storedEtag)) {
                notModified.setHeader(S3Dispatcher.HTTP_HEADER_NAME_ETAG, quoteEtag(storedEtag.toLowerCase()));
            }
            notModified.setHeader(HttpHeaderNames.LAST_MODIFIED,
                                  Outcall.RFC2616_INSTANT.format(Instant.ofEpochMilli(object.getFile()
                                                                                            .lastModified())));
            notModified.status(HttpResponseStatus.NOT_MODIFIED);
            responder.signalObjectSuccess(webContext);
            return;
        }

        // The conditions are settled. Sending the file would evaluate this one again, and on its own, without
        // knowing that If-None-Match takes precedence over it.
        webContext.getRequest().headers().remove(HttpHeaderNames.IF_MODIFIED_SINCE);

        if (!applyRange(webContext, object)) {
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        id,
                                        S3ErrorCode.InvalidRange,
                                        "The requested range is not satisfiable");
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

    /**
     * Checks the range of a request against the object, and adapts it to what sending a file expects.
     * <p>
     * S3 serves what is there of a range which reaches beyond the end of an object, and the whole object for a suffix
     * which is longer than it, whereas sending the file would refuse the former and fail on the latter. Such a range
     * is therefore cut to the size of the object. Everything else is left to sending the file.
     *
     * @param webContext the context describing the current request
     * @param object     the object to read
     * @return <tt>false</tt> if the range lies entirely outside of the object, <tt>true</tt> otherwise
     */
    private boolean applyRange(WebContext webContext, StoredObject object) {
        String range = webContext.getHeaderValue(HttpHeaderNames.RANGE).asString("").trim();
        long size = object.getSizeBytes();

        Matcher suffixMatcher = SUFFIX_RANGE_PATTERN.matcher(range);
        if (suffixMatcher.matches()) {
            long length = Long.parseLong(suffixMatcher.group(1));
            if (length == 0 || size == 0) {
                return false;
            }
            if (length >= size) {
                webContext.getRequest().headers().set(HttpHeaderNames.RANGE, "bytes=0-" + (size - 1));
            }
            return true;
        }

        Matcher rangeMatcher = OPEN_RANGE_PATTERN.matcher(range);
        if (rangeMatcher.matches()) {
            if (Long.parseLong(rangeMatcher.group(1)) >= size) {
                return false;
            }
            if (Strings.isFilled(rangeMatcher.group(2)) && Long.parseLong(rangeMatcher.group(2)) >= size) {
                webContext.getRequest()
                          .headers()
                          .set(HttpHeaderNames.RANGE, "bytes=" + rangeMatcher.group(1) + "-" + (size - 1));
            }
        }
        return true;
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
