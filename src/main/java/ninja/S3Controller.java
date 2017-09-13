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
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.ConfigValue;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Counter;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;
import sirius.kernel.xml.Attribute;
import sirius.kernel.xml.XMLReader;
import sirius.kernel.xml.XMLStructuredOutput;
import sirius.web.controller.Controller;
import sirius.web.controller.Routed;
import sirius.web.http.InputStreamHandler;
import sirius.web.http.Response;
import sirius.web.http.WebContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpMethod.*;
import static ninja.Aws4HashCalculator.AWS_AUTH4_PATTERN;
import static ninja.AwsHashCalculator.AWS_AUTH_PATTERN;

/**
 * Handles calls to the S3 API.
 */
@Register
public class S3Controller implements Controller {

    public static final String HTTP_HEADER_NAME_ETAG = "ETag";

    @Override
    public void onError(WebContext ctx, HandledException error) {
        signalObjectError(ctx, HttpResponseStatus.BAD_REQUEST, error.getMessage());
    }

    @Part
    private Storage storage;

    @Part
    private APILog log;

    @Part
    private AwsHashCalculator hashCalculator;

    @ConfigValue("storage.multipartDir")
    private String multipartDir;

    private Set<String> multipartUploads = Collections.synchronizedSet(new TreeSet<>());

    private Counter uploadIdCounter = new Counter();

    public static final DateTimeFormatter ISO_INSTANT =
            new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                                          .toFormatter()
                                          .withChronology(IsoChronology.INSTANCE)
                                          .withZone(ZoneOffset.UTC);

    private static final Map<String, String> headerOverrides;

    static {
        headerOverrides = Maps.newTreeMap();
        headerOverrides.put("response-content-type", "Content-Type");
        headerOverrides.put("response-content-language", "Content-Language");
        headerOverrides.put("response-expires", "Expires");
        headerOverrides.put("response-cache-control", "Cache-Control");
        headerOverrides.put("response-content-disposition", "Content-Disposition");
        headerOverrides.put("response-content-encoding", "Content-Encoding");
    }

    /**
     * Extracts the given hash from the given request. Returns null if no hash was given.
     */
    private String getAuthHash(WebContext ctx) {
        Value authorizationHeaderValue = ctx.getHeaderValue(HttpHeaderNames.AUTHORIZATION);
        if (!authorizationHeaderValue.isFilled()) {
            return ctx.get("Signature").getString();
        }
        String authentication =
                Strings.isEmpty(authorizationHeaderValue.getString()) ? "" : authorizationHeaderValue.getString();
        Matcher m = AWS_AUTH_PATTERN.matcher(authentication);
        if (m.matches()) {
            return m.group(2);
        }

        m = AWS_AUTH4_PATTERN.matcher(authentication);
        if (m.matches()) {
            return m.group(5);
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
        log.log("OBJECT " + ctx.getRequest().method().name(),
                message + " - " + ctx.getRequestedURI(),
                APILog.Result.ERROR,
                CallContext.getCurrent().getWatch());
    }

    /**
     * Writes an API success entry to the log
     */
    private void signalObjectSuccess(WebContext ctx) {
        log.log("OBJECT " + ctx.getRequest().method().name(),
                ctx.getRequestedURI(),
                APILog.Result.OK,
                CallContext.getCurrent().getWatch());
    }

    /**
     * GET a list of all buckets
     *
     * @param ctx the context describing the current request
     */
    @Routed(value = "/s3", priority = 99)
    public void listBuckets(WebContext ctx) {
        HttpMethod method = ctx.getRequest().method();

        if (GET == method) {
            List<Bucket> buckets = storage.getBuckets();
            Response response = ctx.respondWith();

            response.setHeader("Content-Type", "application/xml");

            XMLStructuredOutput out = response.xml();
            out.beginOutput("ListAllMyBucketsResult",
                            Attribute.set("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/"));
            out.beginObject("Owner");
            out.property("ID", "initiatorId");
            out.property("DisplayName", "initiatorName");
            out.endObject();

            out.beginObject("Buckets");
            for (Bucket bucket : buckets) {
                out.beginObject("Bucket");
                out.property("Name", bucket.getName());
                out.property("CreationDate", ISO_INSTANT.format(Instant.ofEpochMilli(bucket.getFile().lastModified())));
                out.endObject();
            }
            out.endObject();
            out.endOutput();
        } else {
            throw new IllegalArgumentException(ctx.getRequest().method().name());
        }
    }

    /**
     * Dispatching method handling bucket specific calls without content (HEAD and DELETE)
     *
     * @param ctx        the context describing the current request
     * @param bucketName name of the bucket of interest
     */
    @Routed(value = "/s3/:1", priority = 99)
    public void bucket(WebContext ctx, String bucketName) {
        Bucket bucket = storage.getBucket(bucketName);

        HttpMethod method = ctx.getRequest().method();

        if (HEAD == method) {
            if (bucket.exists()) {
                signalObjectSuccess(ctx);
                ctx.respondWith().status(HttpResponseStatus.OK);
            } else {
                signalObjectError(ctx, HttpResponseStatus.NOT_FOUND, "Bucket does not exist");
            }
        } else if (GET == method) {
            if (bucket.exists()) {
                listObjects(ctx, bucket);
            } else {
                signalObjectError(ctx, HttpResponseStatus.NOT_FOUND, "Bucket does not exist");
            }
        } else if (DELETE == method) {
            bucket.delete();
            signalObjectSuccess(ctx);
            ctx.respondWith().status(HttpResponseStatus.OK);
        } else {
            throw new IllegalArgumentException(ctx.getRequest().method().name());
        }
    }

    /**
     * Dispatching method handling bucket specific calls with content (PUT)
     *
     * @param ctx        the context describing the current request
     * @param bucketName name of the bucket of interest
     * @param in         input stream with the requests content
     */
    @Routed(value = "/s3/:1", priority = 99, preDispatchable = true)
    public void bucket(WebContext ctx, String bucketName, InputStreamHandler in) {
        Bucket bucket = storage.getBucket(bucketName);

        HttpMethod method = ctx.getRequest().method();
        if (PUT == method) {
            bucket.create();
            signalObjectSuccess(ctx);
            ctx.respondWith().status(HttpResponseStatus.OK);
        } else {
            throw new IllegalArgumentException(ctx.getRequest().method().name());
        }
    }

    /**
     * Dispatching method handling all object specific calls.
     *
     * @param ctx        the context describing the current request
     * @param bucketName name of the bucket which contains the object (must exist)
     * @param objectId   name of the object of interest
     * @param idList     list of object names if the reequest was for multiple objects
     * @throws Exception in case of IO errors and there like
     */
    @Routed("/s3/:1/:2/**")
    public void object(WebContext ctx, String bucketName, String objectId, List<String> idList) throws Exception {
        Bucket bucket = storage.getBucket(bucketName);
        String id = getIdsAsString(objectId, idList);
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
                abortMultipartUpload(ctx, bucket, id, uploadId);
            } else {
                deleteObject(ctx, bucket, id);
            }
        } else {
            throw new IllegalArgumentException(ctx.getRequest().method().name());
        }
    }

    /**
     * Dispatching method handling all object specific calls.
     *
     * @param ctx        the context describing the current request
     * @param bucketName name of the bucket which contains the object (must exist)
     * @param objectId   name of the object of interest
     * @param idList     list of object names if the reequest was for multiple objects
     * @param in         input stream with the requests content
     * @throws Exception in case of IO errors and there like
     */
    @Routed(value = "/s3/:1/:2/**", preDispatchable = true)
    public void object(WebContext ctx, String bucketName, String objectId, List<String> idList, InputStreamHandler in)
            throws Exception {
        Bucket bucket = storage.getBucket(bucketName);
        String id = getIdsAsString(objectId, idList);
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
                multiObject(ctx, bucket, id, uploadId, ctx.get("partNumber").asString(), in);
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
                signalObjectError(ctx, HttpResponseStatus.NOT_FOUND, "Bucket does not exist");
                return false;
            }
        }
        return true;
    }

    private static String getIdsAsString(String objectId, List<String> idList) {
        List<String> ids = new ArrayList<>();
        ids.add(objectId);
        ids.addAll(idList);
        return ids.stream().filter(i -> Strings.isFilled(i)).collect(Collectors.joining("/")).replace('/', '_');
    }

    private boolean objectCheckAuth(WebContext ctx, Bucket bucket) {
        String hash = getAuthHash(ctx);
        if (ctx.getRequestedURI().contains("#")) {
            return true;
        }
        if (hash != null) {
            String expectedHash = computeHash(ctx, "");
            String alternativeHash = computeHash(ctx, "/s3");
            if (!expectedHash.equals(hash) && !alternativeHash.equals(hash)) {
                ctx.respondWith()
                   .error(HttpResponseStatus.UNAUTHORIZED,
                          Strings.apply("Invalid Hash (Expected: %s, Found: %s)", expectedHash, hash));
                log.log("OBJECT " + ctx.getRequest().method().name(),
                        ctx.getRequestedURI(),
                        APILog.Result.REJECTED,
                        CallContext.getCurrent().getWatch());
                return false;
            }
        }
        if (bucket.isPrivate() && !ctx.get("noAuth").isFilled() && hash == null) {
            ctx.respondWith().error(HttpResponseStatus.UNAUTHORIZED, "Authentication required");
            log.log("OBJECT " + ctx.getRequest().method().name(),
                    ctx.getRequestedURI(),
                    APILog.Result.REJECTED,
                    CallContext.getCurrent().getWatch());
            return false;
        }

        return true;
    }

    private String computeHash(WebContext ctx, String pathPrefix) {
        return hashCalculator.computeHash(ctx, pathPrefix);
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
        response.setHeader("Content-Type", "application/xml");

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
    private void putObject(WebContext ctx, Bucket bucket, String id, InputStreamHandler inputStream) throws Exception {
        StoredObject object = bucket.getObject(id);
        if (inputStream == null) {
            signalObjectError(ctx, HttpResponseStatus.BAD_REQUEST, "No content posted");
            return;
        }
        try (FileOutputStream out = new FileOutputStream(object.getFile())) {
            ByteStreams.copy(inputStream, out);
        }

        Map<String, String> properties = Maps.newTreeMap();
        for (String name : ctx.getRequest().headers().names()) {
            String nameLower = name.toLowerCase();
            if (nameLower.startsWith("x-amz-meta-") || "content-md5".equals(nameLower) || "content-type".equals(
                    nameLower) || "x-amz-acl".equals(nameLower)) {
                properties.put(name, ctx.getHeader(name));
            }
        }
        HashCode hash = Files.hash(object.getFile(), Hashing.md5());
        String md5 = BaseEncoding.base64().encode(hash.asBytes());
        if (properties.containsKey("Content-MD5")) {
            if (!md5.equals(properties.get("Content-MD5"))) {
                object.delete();
                signalObjectError(ctx,
                                  HttpResponseStatus.BAD_REQUEST,
                                  Strings.apply("Invalid MD5 checksum (Input: %s, Expected: %s)",
                                                properties.get("Content-MD5"),
                                                md5));
                return;
            }
        }

        object.storeProperties(properties);
        Response response = ctx.respondWith();
        response.addHeader(HTTP_HEADER_NAME_ETAG, etag(hash)).status(HttpResponseStatus.OK);
        response.addHeader(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS, HTTP_HEADER_NAME_ETAG);
        signalObjectSuccess(ctx);
    }

    private String etag(HashCode hash) {
        return "\"" + hash + "\"";
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
        if (!copy.contains("/")) {
            signalObjectError(ctx, HttpResponseStatus.BAD_REQUEST, "Source must contain '/'");
            return;
        }
        String srcBucketName = copy.substring(1, copy.lastIndexOf("/"));
        String srcId = copy.substring(copy.lastIndexOf("/") + 1);
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
        String etag = etag(hash);
        XMLStructuredOutput structuredOutput = ctx.respondWith().addHeader(HTTP_HEADER_NAME_ETAG, etag).xml();
        structuredOutput.beginOutput("CopyObjectResult");
        structuredOutput.beginObject("LastModified");
        structuredOutput.text(ISO_INSTANT.format(object.getLastModifiedInstant()));
        structuredOutput.endObject();
        structuredOutput.beginObject(HTTP_HEADER_NAME_ETAG);
        structuredOutput.text(etag);
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
    private void getObject(WebContext ctx, Bucket bucket, String id, boolean sendFile) throws Exception {
        StoredObject object = bucket.getObject(id);
        if (!object.exists()) {
            signalObjectError(ctx, HttpResponseStatus.NOT_FOUND, "Object does not exist");
            return;
        }
        Response response = ctx.respondWith();
        for (Map.Entry<Object, Object> entry : object.getProperties()) {
            response.addHeader(entry.getKey().toString(), entry.getValue().toString());
        }
        for (Map.Entry<String, String> entry : getOverridenHeaders(ctx).entrySet()) {
            response.setHeader(entry.getKey(), entry.getValue());
        }
        HashCode hash = Files.hash(object.getFile(), Hashing.md5());
        response.addHeader(HTTP_HEADER_NAME_ETAG, BaseEncoding.base16().encode(hash.asBytes()));
        response.addHeader(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS, HTTP_HEADER_NAME_ETAG);
        if (sendFile) {
            response.file(object.getFile());
        } else {
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
        response.setHeader("Content-Type", "application/xml");

        String uploadId = String.valueOf(uploadIdCounter.inc());
        multipartUploads.add(uploadId);

        getUploadDir(uploadId).mkdirs();

        XMLStructuredOutput out = response.xml();
        out.beginOutput("InitiateMultipartUploadResult");
        out.property("Bucket", bucket.getName());
        out.property("Key", id);
        out.property("UploadId", uploadId);
        out.endOutput();
    }

    /**
     * Handles PUT /bucket/id?uploadId=X&partNumber=Y
     *
     * @param ctx        the context describing the current request
     * @param bucket     the bucket containing the object to upload
     * @param id         name of the object to upload
     * @param uploadId   the multipart upload this part belongs to
     * @param partNumber the number of this part in the complete upload
     * @param part       input stream with the content of this part
     */
    private void multiObject(WebContext ctx,
                             Bucket bucket,
                             String id,
                             String uploadId,
                             String partNumber,
                             InputStreamHandler part) {
        if (!multipartUploads.contains(uploadId)) {
            ctx.respondWith().error(HttpResponseStatus.NOT_FOUND, "Multipart Upload does not exist");
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
            ctx.respondWith().error(HttpResponseStatus.NOT_FOUND, "Multipart Upload does not exist");
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

        File file = combineParts(id, uploadId, parts);
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

            XMLStructuredOutput out = ctx.respondWith().xml();
            out.beginOutput("CompleteMultipartUploadResult");
            out.property("Location", "");
            out.property("Bucket", bucket.getName());
            out.property("Key", id);
            out.property(HTTP_HEADER_NAME_ETAG, etag);
            out.endOutput();
        } catch (IOException e) {
            Exceptions.ignore(e);
            ctx.respondWith().error(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Could not build response");
        }
    }

    private File getUploadDir(String uploadId) {
        return new File(multipartDir + "/" + uploadId);
    }

    private File combineParts(String id, String uploadId, Map<Integer, File> parts) {
        File file = new File(getUploadDir(uploadId), id);
        ByteBuffer[] buffers = new ByteBuffer[parts.size()];

        try {
            for (Map.Entry<Integer, File> entry : parts.entrySet()) {
                try (RandomAccessFile raf = new RandomAccessFile(entry.getValue(), "r")) {
                    FileChannel channel = raf.getChannel();
                    buffers[entry.getKey() - 1] = channel.map(FileChannel.MapMode.READ_ONLY, 0, raf.length());
                }
            }
            file.createNewFile();
            FileOutputStream outFile = new FileOutputStream(file);
            try (FileChannel out = outFile.getChannel()) {
                out.write(buffers);
            }
        } catch (IOException e) {
            Exceptions.handle(e);
        }

        return file;
    }

    /**
     * Handles DELETE /bucket/id?uploadId=X
     *
     * @param ctx      the context describing the current request
     * @param bucket   the bucket containing the object to upload
     * @param id       name of the object to upload
     * @param uploadId the multipart upload that should be cancelled
     */
    private void abortMultipartUpload(WebContext ctx, Bucket bucket, String id, String uploadId) {
        System.out.println("aborting multiupload");
        multipartUploads.remove(uploadId);
        ctx.respondWith().status(HttpResponseStatus.OK);

        File uploadDir = getUploadDir(uploadId);
        delete(uploadDir);
    }

    private static void delete(File file) {
        if (file.isDirectory()) {
            if (file.list().length == 0) {
                file.delete();
            } else {
                String[] files = file.list();

                for (String temp : files) {
                    delete(new File(file, temp));
                }
                delete(file);
            }
        } else {
            file.delete();
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
            ctx.respondWith().error(HttpResponseStatus.NOT_FOUND, "Multipart Upload does not exist");
            return;
        }

        Response response = ctx.respondWith();

        response.setHeader("Content-Type", "application/xml");

        XMLStructuredOutput out = response.xml();
        out.beginOutput("ListPartsResult");
        out.property("Bucket", bucket.getName());
        out.property("Key", id);
        out.property("UploadId", uploadId);

        out.beginObject("Initiator");
        out.property("ID", "initiatorId");
        out.property("DisplayName", "initiatorName");
        out.endObject();

        out.beginObject("Owner");
        out.property("ID", "initiatorId");
        out.property("DisplayName", "initiatorName");
        out.endObject();

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
            out.property("LastModified", ISO_INSTANT.format(Instant.ofEpochMilli(part.lastModified())));
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
            String header = entry.getValue().toString();
            String val = ctx.getParameter(entry.getKey().toString());
            if (val != null) {
                overrides.put(header, val);
            }
        }
        return overrides;
    }
}
