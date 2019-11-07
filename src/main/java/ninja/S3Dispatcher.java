/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import sirius.kernel.async.CallContext;
import sirius.kernel.commons.Callback;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.ConfigValue;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Counter;
import sirius.kernel.health.Exceptions;
import sirius.kernel.xml.Attribute;
import sirius.kernel.xml.XMLReader;
import sirius.kernel.xml.XMLStructuredOutput;
import sirius.web.http.InputStreamHandler;
import sirius.web.http.MimeHelper;
import sirius.web.http.Response;
import sirius.web.http.WebContext;
import sirius.web.http.WebDispatcher;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpMethod.*;
import static ninja.Aws4HashCalculator.AWS_AUTH4_PATTERN;
import static ninja.AwsHashCalculator.AWS_AUTH_PATTERN;

/**
 * Handles S3 API Calls.
 */
@Register
public class S3Dispatcher implements WebDispatcher {

    private static final String HTTP_HEADER_NAME_ETAG = "ETag";
    private static final String HTTP_HEADER_NAME_CONTENT_TYPE = "Content-Type";
    private static final String CONTENT_TYPE_XML = "application/xml";
    private static final String RESPONSE_DISPLAY_NAME = "DisplayName";
    private static final String RESPONSE_BUCKET = "Bucket";
    private static final String ERROR_MULTIPART_UPLOAD_DOES_NOT_EXIST = "Multipart Upload does not exist";
    private static final String ERROR_BUCKET_DOES_NOT_EXIST = "Bucket does not exist";
    private static final String PATH_DELIMITER = "/";

    @Part
    private APILog log;

    @Part
    private AwsHashCalculator hashCalculator;

    @ConfigValue("storage.multipartDir")
    private String multipartDir;

    private Set<String> multipartUploads = Collections.synchronizedSet(new TreeSet<>());

    private Counter uploadIdCounter = new Counter();

    /**
     * Formatter to create appropriate timestamps as expected by AWS...
     */
    public static final DateTimeFormatter RFC822_INSTANT =
            new DateTimeFormatterBuilder().appendPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'")
                                          .toFormatter()
                                          .withLocale(Locale.ENGLISH)
                                          .withChronology(IsoChronology.INSTANCE)
                                          .withZone(ZoneOffset.ofHours(0));

    private static final Map<String, String> headerOverrides;

    static {
        headerOverrides = Maps.newTreeMap();
        headerOverrides.put("response-content-type", HTTP_HEADER_NAME_CONTENT_TYPE);
        headerOverrides.put("response-content-language", "Content-Language");
        headerOverrides.put("response-expires", "Expires");
        headerOverrides.put("response-cache-control", "Cache-Control");
        headerOverrides.put("response-content-disposition", "Content-Disposition");
        headerOverrides.put("response-content-encoding", "Content-Encoding");
    }

    @Part
    private Storage storage;

    @Part
    private Aws4HashCalculator aws4HashCalculator;

    @Override
    public int getPriority() {
        return 800;
    }

    @Override
    public Callback<WebContext> preparePreDispatch(WebContext ctx) {
        String uri = getEffectiveURI(ctx);
        Tuple<String, String> bucketAndObject = Strings.split(uri, "/");
        if (Strings.isEmpty(bucketAndObject.getSecond())) {
            return null;
        }

        Bucket bucket = storage.getBucket(bucketAndObject.getFirst());
        if (!bucket.exists() && !storage.isAutocreateBuckets()) {
            return null;
        }

        InputStreamHandler handler = createInputStreamHandler(ctx);
        ctx.setContentHandler(handler);
        return req -> writeObject(req, bucketAndObject.getFirst(), bucketAndObject.getSecond(), handler);
    }

    private InputStreamHandler createInputStreamHandler(WebContext ctx) {
        if (aws4HashCalculator.supports(ctx) && ctx.getRequest().method() == PUT) {
            return new SignedChunkHandler();
        } else {
            return new InputStreamHandler();
        }
    }

    /**
     * Returns the effective URI.
     * <p>
     * As we have to support legacy URIs which have an <tt>/s3</tt> prefix, we cut this here, and
     * also the first "/" and only return the effective URI to process.
     *
     * @param ctx the current request
     * @return the effective URI to process
     */
    public static String getEffectiveURI(WebContext ctx) {
        String uri = ctx.getRequestedURI();
        if (uri.startsWith("/s3")) {
            uri = uri.substring(3);
        }
        if (uri.startsWith("/")) {
            uri = uri.substring(1);
        }

        return uri;
    }

    @Override
    public boolean dispatch(WebContext ctx) throws Exception {
        String uri = getEffectiveURI(ctx);
        if (Strings.isEmpty(uri)) {
            listBuckets(ctx);
            return true;
        }

        Tuple<String, String> bucketAndObject = Strings.split(uri, "/");
        if (Strings.isEmpty(bucketAndObject.getSecond())) {
            bucket(ctx, bucketAndObject.getFirst());
            return true;
        }

        Bucket bucket = storage.getBucket(bucketAndObject.getFirst());
        if (!bucket.exists() && !storage.isAutocreateBuckets()) {
            return false;
        }

        readObject(ctx, bucketAndObject.getFirst(), bucketAndObject.getSecond());
        return true;
    }

    /**
     * Extracts the given hash from the given request. Returns null if no hash was given.
     */
    private String getAuthHash(WebContext ctx) {
        Value authorizationHeaderValue = ctx.getHeaderValue(HttpHeaderNames.AUTHORIZATION);
        if (!authorizationHeaderValue.isFilled()) {
            return ctx.get("Signature").asString(ctx.get("X-Amz-Signature").asString());
        }
        String authentication =
                Strings.isEmpty(authorizationHeaderValue.getString()) ? "" : authorizationHeaderValue.getString();
        Matcher m = AWS_AUTH_PATTERN.matcher(authentication);
        if (m.matches()) {
            return m.group(2);
        }

        m = AWS_AUTH4_PATTERN.matcher(authentication);
        if (m.matches()) {
            return m.group(7);
        }

        return null;
    }

    /**
     * Writes an API error to the log
     */
    private void signalObjectError(WebContext ctx, HttpResponseStatus status, String message) {
        if (ctx.getRequest().method() == HEAD) {
            ctx.respondWith().status(status);
        } else {
            ctx.respondWith().error(status, message);
        }
        log.log(ctx.getRequest().method().name(),
                message + " - " + ctx.getRequestedURI(),
                APILog.Result.ERROR,
                CallContext.getCurrent().getWatch());
    }

    /**
     * Writes an API success entry to the log
     */
    private void signalObjectSuccess(WebContext ctx) {
        log.log(ctx.getRequest().method().name(),
                ctx.getRequestedURI(),
                APILog.Result.OK,
                CallContext.getCurrent().getWatch());
    }

    /**
     * GET a list of all buckets
     *
     * @param ctx the context describing the current request
     */
    private void listBuckets(WebContext ctx) {
        HttpMethod method = ctx.getRequest().method();

        if (GET == method) {
            List<Bucket> buckets = storage.getBuckets();
            Response response = ctx.respondWith();

            response.setHeader(HTTP_HEADER_NAME_CONTENT_TYPE, CONTENT_TYPE_XML);

            XMLStructuredOutput out = response.xml();
            out.beginOutput("ListAllMyBucketsResult",
                            Attribute.set("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/"));
            out.property("hint", "Goto: " + ctx.getBaseURL() + "/ui to visit the admin UI");
            outputOwnerInfo(out, "Owner");

            out.beginObject("Buckets");
            for (Bucket bucket : buckets) {
                out.beginObject(RESPONSE_BUCKET);
                out.property("Name", bucket.getName());
                out.property("CreationDate",
                             RFC822_INSTANT.format(Instant.ofEpochMilli(bucket.getFile().lastModified())));
                out.endObject();
            }
            out.endObject();
            out.endOutput();
        } else {
            throw new IllegalArgumentException(ctx.getRequest().method().name());
        }
    }

    private void outputOwnerInfo(XMLStructuredOutput out, String name) {
        out.beginObject(name);
        out.property("ID", "initiatorId");
        out.property(RESPONSE_DISPLAY_NAME, "initiatorName");
        out.endObject();
    }

    /**
     * Dispatching method handling bucket specific calls without content (HEAD, DELETE, GET and PUT)
     *
     * @param ctx        the context describing the current request
     * @param bucketName name of the bucket of interest
     */
    private void bucket(WebContext ctx, String bucketName) {
        Bucket bucket = storage.getBucket(bucketName);

        if (!objectCheckAuth(ctx, bucket)) {
            return;
        }

        HttpMethod method = ctx.getRequest().method();

        if (HEAD == method) {
            if (bucket.exists()) {
                signalObjectSuccess(ctx);
                ctx.respondWith().status(HttpResponseStatus.OK);
            } else {
                signalObjectError(ctx, HttpResponseStatus.NOT_FOUND, ERROR_BUCKET_DOES_NOT_EXIST);
            }
        } else if (GET == method) {
            if (bucket.exists()) {
                listObjects(ctx, bucket);
            } else {
                signalObjectError(ctx, HttpResponseStatus.NOT_FOUND, ERROR_BUCKET_DOES_NOT_EXIST);
            }
        } else if (DELETE == method) {
            bucket.delete();
            signalObjectSuccess(ctx);
            ctx.respondWith().status(HttpResponseStatus.OK);
        } else if (PUT == method) {
            bucket.create();
            signalObjectSuccess(ctx);
            ctx.respondWith().status(HttpResponseStatus.OK);
        } else {
            throw new IllegalArgumentException(ctx.getRequest().method().name());
        }
    }

    /**
     * Dispatching method handling all object specific calls which either read or delete the object but do not provide
     * any data.
     *
     * @param ctx        the context describing the current request
     * @param bucketName name of the bucket which contains the object (must exist)
     * @param objectId   name of the object of interest
     * @throws IOException in case of IO errors and there like
     */
    private void readObject(WebContext ctx, String bucketName, String objectId) throws IOException {
        Bucket bucket = storage.getBucket(bucketName);
        String id = objectId.replace('/', '_');
        String uploadId = ctx.get("uploadId").asString();

        if (!checkObjectRequest(ctx, bucket, id)) {
            return;
        }

        HttpMethod method = ctx.getRequest().method();
        if (HEAD == method) {
            getObject(ctx, bucket, id, false);
        } else if (GET == method) {
            if (Strings.isFilled(uploadId)) {
                getPartList(ctx, bucket, id, uploadId);
            } else {
                getObject(ctx, bucket, id, true);
            }
        } else if (DELETE == method) {
            if (Strings.isFilled(uploadId)) {
                abortMultipartUpload(ctx, uploadId);
            } else {
                deleteObject(ctx, bucket, id);
            }
        } else {
            throw new IllegalArgumentException(ctx.getRequest().method().name());
        }
    }

    /**
     * Dispatching method handling all object specific calls which write / provide data.
     *
     * @param ctx        the context describing the current request
     * @param bucketName name of the bucket which contains the object (must exist)
     * @param objectId   name of the object of interest
     * @param in         the data to process
     * @throws IOException in case of IO errors and there like
     */
    private void writeObject(WebContext ctx, String bucketName, String objectId, InputStreamHandler in)
            throws IOException {
        Bucket bucket = storage.getBucket(bucketName);
        String id = objectId.replace('/', '_');
        String uploadId = ctx.get("uploadId").asString();

        if (!checkObjectRequest(ctx, bucket, id)) {
            return;
        }

        HttpMethod method = ctx.getRequest().method();
        if (PUT == method) {
            Value copy = ctx.getHeaderValue("x-amz-copy-source");
            if (copy.isFilled()) {
                copyObject(ctx, bucket, id, copy.asString());
            } else if (ctx.hasParameter("partNumber") && Strings.isFilled(uploadId)) {
                multiObject(ctx, uploadId, ctx.get("partNumber").asString(), in);
            } else {
                putObject(ctx, bucket, id, in);
            }
        } else if (POST == method) {
            if (ctx.hasParameter("uploads")) {
                startMultipartUpload(ctx, bucket, id);
            } else if (Strings.isFilled(uploadId)) {
                completeMultipartUpload(ctx, bucket, id, uploadId, in);
            }
        } else {
            throw new IllegalArgumentException(ctx.getRequest().method().name());
        }
    }

    private boolean checkObjectRequest(WebContext ctx, Bucket bucket, String id) {
        if (Strings.isEmpty(id)) {
            signalObjectError(ctx, HttpResponseStatus.NOT_FOUND, "Please provide an object id");
            return false;
        }
        if (!objectCheckAuth(ctx, bucket)) {
            return false;
        }

        if (!bucket.exists()) {
            if (storage.isAutocreateBuckets()) {
                bucket.create();
            } else {
                signalObjectError(ctx, HttpResponseStatus.NOT_FOUND, ERROR_BUCKET_DOES_NOT_EXIST);
                return false;
            }
        }
        return true;
    }

    private boolean objectCheckAuth(WebContext ctx, Bucket bucket) {
        String hash = getAuthHash(ctx);
        if (hash != null) {
            String expectedHash = hashCalculator.computeHash(ctx, "");
            String alternativeHash = hashCalculator.computeHash(ctx, "/s3");
            if (!expectedHash.equals(hash) && !alternativeHash.equals(hash)) {
                ctx.respondWith()
                   .error(HttpResponseStatus.UNAUTHORIZED,
                          Strings.apply("Invalid Hash (Expected: %s, Found: %s)", expectedHash, hash));
                log.log(ctx.getRequest().method().name(),
                        ctx.getRequestedURI(),
                        APILog.Result.REJECTED,
                        CallContext.getCurrent().getWatch());
                return false;
            }
        }
        if (bucket.isPrivate() && !ctx.get("noAuth").isFilled() && hash == null) {
            ctx.respondWith().error(HttpResponseStatus.UNAUTHORIZED, "Authentication required");
            log.log(ctx.getRequest().method().name(),
                    ctx.getRequestedURI(),
                    APILog.Result.REJECTED,
                    CallContext.getCurrent().getWatch());
            return false;
        }

        return true;
    }

    /**
     * Handles GET /bucket
     *
     * @param ctx    the context describing the current request
     * @param bucket the bucket of which the contents should be listed
     */
    private void listObjects(WebContext ctx, Bucket bucket) {
        int maxKeys = ctx.get("max-keys").asInt(1000);
        String marker = ctx.get("marker").asString();
        String prefix = ctx.get("prefix").asString();

        Response response = ctx.respondWith();
        response.setHeader(HTTP_HEADER_NAME_CONTENT_TYPE, CONTENT_TYPE_XML);

        bucket.outputObjects(response.xml(), maxKeys, marker, prefix);
    }

    /**
     * Handles DELETE /bucket/id
     *
     * @param ctx    the context describing the current request
     * @param bucket the bucket containing the object to delete
     * @param id     name of the object to delete
     */
    private void deleteObject(final WebContext ctx, final Bucket bucket, final String id) {
        StoredObject object = bucket.getObject(id);
        object.delete();

        ctx.respondWith().status(HttpResponseStatus.OK);
        signalObjectSuccess(ctx);
    }

    /**
     * Handles PUT /bucket/id
     *
     * @param ctx    the context describing the current request
     * @param bucket the bucket containing the object to upload
     * @param id     name of the object to upload
     */
    private void putObject(WebContext ctx, Bucket bucket, String id, InputStreamHandler inputStream)
            throws IOException {
        StoredObject object = bucket.getObject(id);
        if (inputStream == null) {
            signalObjectError(ctx, HttpResponseStatus.BAD_REQUEST, "No content posted");
            return;
        }
        try (FileOutputStream out = new FileOutputStream(object.getFile())) {
            ByteStreams.copy(inputStream, out);
        }

        Map<String, String> properties = parseUploadProperties(ctx);
        HashCode hash = Files.hash(object.getFile(), Hashing.md5());
        String md5 = BaseEncoding.base64().encode(hash.asBytes());
        String contentMd5 = properties.get("Content-MD5");
        if (properties.containsKey("Content-MD5") && !md5.equals(contentMd5)) {
            object.delete();
            signalObjectError(ctx,
                              HttpResponseStatus.BAD_REQUEST,
                              Strings.apply("Invalid MD5 checksum (Input: %s, Expected: %s)", contentMd5, md5));
            return;
        }

        String etag = BaseEncoding.base16().encode(hash.asBytes());
        properties.put(HTTP_HEADER_NAME_ETAG, etag);
        object.storeProperties(properties);
        Response response = ctx.respondWith();
        response.addHeader(HTTP_HEADER_NAME_ETAG, etag(etag)).status(HttpResponseStatus.OK);
        response.addHeader(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS, HTTP_HEADER_NAME_ETAG);
        signalObjectSuccess(ctx);
    }

    private Map<String, String> parseUploadProperties(WebContext ctx) {
        Map<String, String> properties = Maps.newTreeMap();
        for (String name : ctx.getRequest().headers().names()) {
            String nameLower = name.toLowerCase();
            if (nameLower.startsWith("x-amz-meta-") || "content-md5".equals(nameLower) || "content-type".equals(
                    nameLower) || "x-amz-acl".equals(nameLower)) {
                properties.put(name, ctx.getHeader(name));
            }
        }
        return properties;
    }

    private String etag(String etag) {
        return "\"" + etag + "\"";
    }

    /**
     * Handles GET /bucket/id with an <tt>x-amz-copy-source</tt> header.
     *
     * @param ctx    the context describing the current request
     * @param bucket the bucket containing the object to use as destination
     * @param id     name of the object to use as destination
     */
    private void copyObject(WebContext ctx, Bucket bucket, String id, String copy) throws IOException {
        StoredObject object = bucket.getObject(id);
        if (!copy.contains(PATH_DELIMITER)) {
            signalObjectError(ctx, HttpResponseStatus.BAD_REQUEST, "Source must contain '/'");
            return;
        }
        String srcBucketName = copy.substring(1, copy.lastIndexOf(PATH_DELIMITER));
        String srcId = copy.substring(copy.lastIndexOf(PATH_DELIMITER) + 1);
        Bucket srcBucket = storage.getBucket(srcBucketName);
        if (!srcBucket.exists()) {
            signalObjectError(ctx, HttpResponseStatus.BAD_REQUEST, "Source bucket does not exist");
            return;
        }
        StoredObject src = srcBucket.getObject(srcId);
        if (!src.exists()) {
            signalObjectError(ctx, HttpResponseStatus.BAD_REQUEST, "Source object does not exist");
            return;
        }
        Files.copy(src.getFile(), object.getFile());
        if (src.getPropertiesFile().exists()) {
            Files.copy(src.getPropertiesFile(), object.getPropertiesFile());
        }
        HashCode hash = Files.hash(object.getFile(), Hashing.md5());
        String etag = BaseEncoding.base16().encode(hash.asBytes());

        XMLStructuredOutput structuredOutput = ctx.respondWith().addHeader(HTTP_HEADER_NAME_ETAG, etag(etag)).xml();
        structuredOutput.beginOutput("CopyObjectResult");
        structuredOutput.beginObject("LastModified");
        structuredOutput.text(RFC822_INSTANT.format(object.getLastModifiedInstant()));
        structuredOutput.endObject();
        structuredOutput.beginObject(HTTP_HEADER_NAME_ETAG);
        structuredOutput.text(etag(etag));
        structuredOutput.endObject();
        structuredOutput.endOutput();
        signalObjectSuccess(ctx);
    }

    /**
     * Handles GET /bucket/id
     *
     * @param ctx    the context describing the current request
     * @param bucket the bucket containing the object to download
     * @param id     name of the object to use as download
     */
    private void getObject(WebContext ctx, Bucket bucket, String id, boolean sendFile) throws IOException {
        StoredObject object = bucket.getObject(id);
        if (!object.exists()) {
            signalObjectError(ctx, HttpResponseStatus.NOT_FOUND, "Object does not exist");
            return;
        }
        Response response = ctx.respondWith();
        Properties properties = object.getProperties();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            response.addHeader(entry.getKey().toString(), entry.getValue().toString());
        }
        for (Map.Entry<String, String> entry : getOverridenHeaders(ctx).entrySet()) {
            response.setHeader(entry.getKey(), entry.getValue());
        }

        String etag = properties.getProperty(HTTP_HEADER_NAME_ETAG);
        if (Strings.isEmpty(etag)) {
            HashCode hash = Files.hash(object.getFile(), Hashing.md5());
            etag = BaseEncoding.base16().encode(hash.asBytes());
            Map<String, String> data = new HashMap<>();
            properties.forEach((key, value) -> data.put(key.toString(), String.valueOf(value)));
            data.put(HTTP_HEADER_NAME_ETAG, etag);
            object.storeProperties(data);
        }

        response.addHeader(HTTP_HEADER_NAME_ETAG, etag(etag));
        response.addHeader(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS, HTTP_HEADER_NAME_ETAG);
        if (sendFile) {
            response.file(object.getFile());
        } else {
            String contentType = MimeHelper.guessMimeType(object.getFile().getName());
            response.addHeader(HttpHeaderNames.CONTENT_TYPE, contentType);
            response.addHeader(HttpHeaderNames.LAST_MODIFIED,
                               RFC822_INSTANT.format(Instant.ofEpochMilli(object.getFile().lastModified())));
            response.addHeader(HttpHeaderNames.CONTENT_LENGTH, object.getFile().length());
            response.status(HttpResponseStatus.OK);
        }
        signalObjectSuccess(ctx);
    }

    /**
     * Handles POST /bucket/id?uploads
     *
     * @param ctx    the context describing the current request
     * @param bucket the bucket containing the object to upload
     * @param id     name of the object to upload
     */
    private void startMultipartUpload(WebContext ctx, Bucket bucket, String id) {
        Response response = ctx.respondWith();

        Map<String, String> properties = Maps.newTreeMap();
        for (String name : ctx.getRequest().headers().names()) {
            String nameLower = name.toLowerCase();
            if (nameLower.startsWith("x-amz-meta-") || "content-md5".equals(nameLower) || "content-type".equals(
                    nameLower) || "x-amz-acl".equals(nameLower)) {
                properties.put(name, ctx.getHeader(name));
                response.addHeader(name, ctx.getHeader(name));
            }
        }
        response.setHeader(HTTP_HEADER_NAME_CONTENT_TYPE, CONTENT_TYPE_XML);

        String uploadId = String.valueOf(uploadIdCounter.inc());
        multipartUploads.add(uploadId);

        getUploadDir(uploadId).mkdirs();

        XMLStructuredOutput out = response.xml();
        out.beginOutput("InitiateMultipartUploadResult");
        out.property(RESPONSE_BUCKET, bucket.getName());
        out.property("Key", id);
        out.property("UploadId", uploadId);
        out.endOutput();
    }

    /**
     * Handles PUT /bucket/id?uploadId=X&partNumber=Y
     *
     * @param ctx        the context describing the current request
     * @param uploadId   the multipart upload this part belongs to
     * @param partNumber the number of this part in the complete upload
     * @param part       input stream with the content of this part
     */
    private void multiObject(WebContext ctx, String uploadId, String partNumber, InputStreamHandler part) {
        if (!multipartUploads.contains(uploadId)) {
            ctx.respondWith().error(HttpResponseStatus.NOT_FOUND, ERROR_MULTIPART_UPLOAD_DOES_NOT_EXIST);
            return;
        }

        String etag = "";

        try {
            File partFile = new File(getUploadDir(uploadId), partNumber);
            partFile.deleteOnExit();
            Files.touch(partFile);

            try (FileOutputStream out = new FileOutputStream(partFile)) {
                ByteStreams.copy(part, out);
            }
            part.close();

            etag = Files.hash(partFile, Hashing.md5()).toString();
        } catch (IOException e) {
            Exceptions.handle(e);
        }

        Response response = ctx.respondWith();
        response.setHeader(HTTP_HEADER_NAME_ETAG, etag);
        response.addHeader(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS, HTTP_HEADER_NAME_ETAG);
        response.status(HttpResponseStatus.OK);
    }

    /**
     * Handles POST /bucket/id?uploadId=X
     *
     * @param ctx      the context describing the current request
     * @param bucket   the bucket containing the object to upload
     * @param id       name of the object to upload
     * @param uploadId the multipart upload that should be completed
     * @param in       input stream with xml listing uploaded parts
     */
    private void completeMultipartUpload(WebContext ctx,
                                         Bucket bucket,
                                         String id,
                                         final String uploadId,
                                         InputStreamHandler in) {
        if (!multipartUploads.remove(uploadId)) {
            ctx.respondWith().error(HttpResponseStatus.NOT_FOUND, ERROR_MULTIPART_UPLOAD_DOES_NOT_EXIST);
            return;
        }

        final Map<Integer, File> parts = new HashMap<>();

        XMLReader reader = new XMLReader();
        reader.addHandler("Part", part -> {
            int number = part.queryValue("PartNumber").asInt(0);
            parts.put(number, new File(getUploadDir(uploadId), String.valueOf(number)));
        });
        try {
            reader.parse(in);
        } catch (IOException e) {
            Exceptions.handle(e);
        }

        File file = combineParts(id,
                                 uploadId,
                                 parts.entrySet()
                                      .stream()
                                      .sorted(Comparator.comparing(Map.Entry::getKey))
                                      .map(Map.Entry::getValue)
                                      .collect(Collectors.toList()));

        file.deleteOnExit();
        if (!file.exists()) {
            ctx.respondWith().error(HttpResponseStatus.NOT_FOUND, "Multipart File does not exist");
            return;
        }
        try {
            StoredObject object = bucket.getObject(id);
            Files.move(file, object.getFile());
            delete(getUploadDir(uploadId));

            String etag = Files.hash(object.getFile(), Hashing.md5()).toString();

            // Update the ETAG of the underlying object...
            Properties properties = object.getProperties();
            Map<String, String> data = new HashMap<>();
            properties.forEach((key, value) -> data.put(key.toString(), String.valueOf(value)));
            data.put(HTTP_HEADER_NAME_ETAG, etag);
            object.storeProperties(data);

            XMLStructuredOutput out = ctx.respondWith().xml();
            out.beginOutput("CompleteMultipartUploadResult");
            out.property("Location", "");
            out.property(RESPONSE_BUCKET, bucket.getName());
            out.property("Key", id);
            out.property(HTTP_HEADER_NAME_ETAG, etag);
            out.endOutput();
        } catch (IOException e) {
            Exceptions.ignore(e);
            ctx.respondWith().error(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Could not build response");
        }
    }

    private File getUploadDir(String uploadId) {
        return new File(multipartDir + PATH_DELIMITER + uploadId);
    }

    private File combineParts(String id, String uploadId, List<File> parts) {
        File file = new File(getUploadDir(uploadId), id);

        try {
            if (!file.createNewFile()) {
                Storage.LOG.WARN("Failed to create multipart result file %s (%s).",

                                 file.getName(), file.getAbsolutePath());
            }
            try (FileChannel out = new FileOutputStream(file).getChannel()) {
                combine(parts, out);
            }
        } catch (IOException e) {
            throw Exceptions.handle(e);
        }

        return file;
    }

    private void combine(List<File> parts, FileChannel out) throws IOException {
        for (File part : parts) {
            try (RandomAccessFile raf = new RandomAccessFile(part, "r")) {
                FileChannel channel = raf.getChannel();
                out.write(channel.map(FileChannel.MapMode.READ_ONLY, 0, raf.length()));
            }
        }
    }

    /**
     * Handles DELETE /bucket/id?uploadId=X
     *
     * @param ctx      the context describing the current request
     * @param uploadId the multipart upload that should be cancelled
     */
    private void abortMultipartUpload(WebContext ctx, String uploadId) {
        multipartUploads.remove(uploadId);
        ctx.respondWith().status(HttpResponseStatus.OK);
        delete(getUploadDir(uploadId));
    }

    private static void delete(File file) {
        try {
            sirius.kernel.commons.Files.delete(file.toPath());
        } catch (IOException e) {
            Exceptions.handle(Storage.LOG, e);
        }
    }

    /**
     * Handles GET /bucket/id?uploadId=uploadId
     *
     * @param ctx    the context describing the current request
     * @param bucket the bucket containing the object to download
     * @param id     name of the object to use as download
     */
    private void getPartList(WebContext ctx, Bucket bucket, String id, String uploadId) {
        if (!multipartUploads.contains(uploadId)) {
            ctx.respondWith().error(HttpResponseStatus.NOT_FOUND, ERROR_MULTIPART_UPLOAD_DOES_NOT_EXIST);
            return;
        }

        Response response = ctx.respondWith();

        response.setHeader(HTTP_HEADER_NAME_CONTENT_TYPE, CONTENT_TYPE_XML);

        XMLStructuredOutput out = response.xml();
        out.beginOutput("ListPartsResult");
        out.property(RESPONSE_BUCKET, bucket.getName());
        out.property("Key", id);
        out.property("UploadId", uploadId);

        outputOwnerInfo(out, "Initiator");
        outputOwnerInfo(out, "Owner");

        File uploadDir = getUploadDir(uploadId);
        int marker = ctx.get("part-number-marker").asInt(0);
        int maxParts = ctx.get("max-parts").asInt(0);

        out.property("StorageClass", "STANDARD");
        out.property("PartNumberMarker", marker);
        if ((marker + maxParts) < uploadDir.list().length) {
            out.property("NextPartNumberMarker", marker + maxParts + 1);
        }

        if (Strings.isFilled(maxParts)) {
            out.property("MaxParts", maxParts);
        }

        boolean truncated = 0 < maxParts && maxParts < uploadDir.list().length;
        out.property("IsTruncated", truncated);

        for (File part : uploadDir.listFiles()) {
            out.beginObject("Part");
            out.property("PartNumber", part.getName());
            out.property("LastModified", RFC822_INSTANT.format(Instant.ofEpochMilli(part.lastModified())));
            try {
                out.property(HTTP_HEADER_NAME_ETAG, Files.hash(part, Hashing.md5()).toString());
            } catch (IOException e) {
                Exceptions.ignore(e);
            }
            out.property("Size", part.length());
            out.endObject();
        }

        out.endOutput();
    }

    private Map<String, String> getOverridenHeaders(WebContext ctx) {
        Map<String, String> overrides = Maps.newTreeMap();
        for (Map.Entry<String, String> entry : headerOverrides.entrySet()) {
            String header = entry.getValue();
            String value = ctx.getParameter(entry.getKey());
            if (value != null) {
                overrides.put(header, value);
            }
        }
        return overrides;
    }
}
