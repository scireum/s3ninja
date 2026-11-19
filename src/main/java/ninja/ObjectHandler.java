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

/**
 * Handles the S3 API calls which deal with single objects: storing, reading, copying and deleting them.
 */
@Register(classes = ObjectHandler.class)
public class ObjectHandler {

    private static final String PATH_DELIMITER = "/";

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
        String contentMd5 = properties.get("Content-MD5");
        if (properties.containsKey("Content-MD5") && !md5.equals(contentMd5)) {
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

    private Map<String, String> parseUploadProperties(WebContext webContext) {
        Map<String, String> properties = Maps.newTreeMap();
        for (String name : webContext.getRequest().headers().names()) {
            String nameLower = name.toLowerCase();
            if (nameLower.startsWith("x-amz-") || "content-md5".equals(nameLower) || "content-type".equals(nameLower)) {
                properties.put(name, webContext.getHeader(name));
            }
        }
        return properties;
    }

    private String quoteEtag(String etag) {
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
        if (Strings.isEmpty(sourcePath) || !sourcePath.contains(PATH_DELIMITER)) {
            responder.signalObjectError(webContext,
                                        null,
                                        null,
                                        S3ErrorCode.InvalidRequest,
                                        String.format("Source '%s' must contain '/'", sourcePath));
            return;
        }

        // parse the path of the source object
        sourcePath = Urls.decode(sourcePath);
        int sourceBucketNameStart = sourcePath.startsWith(PATH_DELIMITER) ? PATH_DELIMITER.length() : 0;
        String sourceBucketName =
                sourcePath.substring(sourceBucketNameStart, sourcePath.indexOf(PATH_DELIMITER, sourceBucketNameStart));
        String sourceKey = sourcePath.substring(sourcePath.indexOf(PATH_DELIMITER, sourceBucketNameStart) + 1);

        Bucket sourceBucket = storage.getBucket(sourceBucketName);
        if (!sourceBucket.exists()) {
            responder.signalObjectError(webContext,
                                        sourceBucketName,
                                        sourceKey,
                                        S3ErrorCode.NoSuchBucket,
                                        String.format("Source bucket '%s' does not exist", sourceBucketName));
            return;
        }

        StoredObject sourceObject = sourceBucket.getObject(sourceKey);
        if (!sourceObject.exists()) {
            responder.signalObjectError(webContext,
                                        sourceBucketName,
                                        sourceKey,
                                        S3ErrorCode.NoSuchKey,
                                        String.format("Source object '%s/%s' does not exist",
                                                      sourceBucketName,
                                                      sourceKey));
            return;
        }

        StoredObject object = bucket.getObject(key);
        Files.copy(sourceObject.getFile(), object.getFile());
        if (sourceObject.getPropertiesFile().exists()) {
            Files.copy(sourceObject.getPropertiesFile(), object.getPropertiesFile());
        }

        String etag = BaseEncoding.base16().encode(Hasher.md5().hashFile(object.getFile()).toHash()).toLowerCase();

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
            response.addHeader(entry.getKey(), entry.getValue());
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
        response.addHeader(S3Dispatcher.HTTP_HEADER_NAME_ETAG, quoteEtag(etag.toLowerCase()));
        response.addHeader(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS, S3Dispatcher.HTTP_HEADER_NAME_ETAG);

        if (sendFile) {
            response.file(object.getFile());
        } else {
            String contentType = MimeHelper.guessMimeType(object.getFile().getName());
            response.addHeader(HttpHeaderNames.CONTENT_TYPE, contentType);
            response.addHeader(HttpHeaderNames.LAST_MODIFIED,
                               Outcall.RFC2616_INSTANT.format(Instant.ofEpochMilli(object.getFile().lastModified())));
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
