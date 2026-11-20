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
import sirius.kernel.commons.Urls;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.xml.XMLStructuredOutput;
import sirius.web.http.InputStreamHandler;
import sirius.web.http.MimeHelper;
import sirius.web.http.Response;
import sirius.web.http.WebContext;

import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Handles the S3 API calls which deal with single objects: storing, reading, copying and deleting them.
 */
@Register(classes = ObjectHandler.class)
public class ObjectHandler {

    private static final String PATH_DELIMITER = "/";

    /**
     * Starts the version which S3 permits at the end of an <tt>x-amz-copy-source</tt> header.
     */
    private static final String COPY_SOURCE_VERSION_SUFFIX = "?versionId=";

    /**
     * Lists the standard headers, in lower case, which are stored along with an object next to all headers starting
     * with {@link S3Dispatcher#AMAZON_HEADER_PREFIX}.
     */
    private static final Set<String> STORED_HEADERS =
            Set.of("cache-control", "content-disposition", "content-md5", "content-type", "expires");

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
        try (FileOutputStream out = new FileOutputStream(object.getFile())) {
            ByteStreams.copy(inputStream, out);
        }

        Map<String, String> properties = parseUploadProperties(webContext);
        byte[] hash = Hasher.md5().hashFile(object.getFile()).toHash();
        String md5 = Base64.getEncoder().encodeToString(hash);
        // Read from the request, not from the properties, which carry the name of the header as spelled by the client.
        String contentMd5 = webContext.getHeader("Content-MD5");
        if (contentMd5 != null && !md5.equals(contentMd5)) {
            object.delete();
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        id,
                                        S3ErrorCode.BadDigest,
                                        Strings.apply("Invalid MD5 checksum (Input: %s, Expected: %s)",
                                                      contentMd5,
                                                      md5));
            return;
        }
        String etag = BaseEncoding.base16().encode(hash).toLowerCase();
        properties.put(S3Dispatcher.HTTP_HEADER_NAME_ETAG, etag);
        object.setProperties(properties);

        Response response = webContext.respondWith();
        response.addHeader(S3Dispatcher.HTTP_HEADER_NAME_ETAG, quoteEtag(etag)).status(HttpResponseStatus.OK);
        response.addHeader(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS, S3Dispatcher.HTTP_HEADER_NAME_ETAG);
        responder.signalObjectSuccess(webContext);
    }

    /**
     * Collects the request headers which are stored along with an object and sent again when it is read.
     * <p>
     * This is used for plain and for multipart uploads alike, so that both keep the same set of headers.
     *
     * @param webContext the context describing the current request
     * @return the headers to store, by their names as sent by the client
     */
    Map<String, String> parseUploadProperties(WebContext webContext) {
        Map<String, String> properties = Maps.newTreeMap();
        for (String name : webContext.getRequest().headers().names()) {
            String nameLower = name.toLowerCase();
            if (nameLower.startsWith(S3Dispatcher.AMAZON_HEADER_PREFIX) || STORED_HEADERS.contains(nameLower)) {
                properties.put(name, webContext.getHeader(name));
            }
        }
        return properties;
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
        Files.copy(sourceObject.getFile(), object.getFile());
        if (sourceObject.getPropertiesFile().exists()) {
            Files.copy(sourceObject.getPropertiesFile(), object.getPropertiesFile());
        }

        // A copy is a plain object, even if its source has been uploaded in parts and thus carries another entity tag.
        String etag = BaseEncoding.base16().encode(Hasher.md5().hashFile(object.getFile()).toHash()).toLowerCase();
        Map<String, String> properties = object.getProperties();
        properties.put(S3Dispatcher.HTTP_HEADER_NAME_ETAG, etag);
        object.setProperties(properties);

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

        Response response = webContext.respondWith();
        Map<String, String> properties = object.getProperties();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            // The entity tag is stored without quotes, and is sent in its proper form below.
            if (!S3Dispatcher.HTTP_HEADER_NAME_ETAG.equalsIgnoreCase(entry.getKey())) {
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
