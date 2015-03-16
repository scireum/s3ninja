/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import sirius.kernel.async.CallContext;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;
import sirius.kernel.xml.XMLStructuredOutput;
import sirius.web.controller.Controller;
import sirius.web.controller.Routed;
import sirius.web.http.Response;
import sirius.web.http.WebContext;
import sirius.web.security.UserContext;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneOffset;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Handles calls to the S3 API.
 *
 * @author Andreas Haufler (aha@scireum.de)
 * @since 2013/08
 */
@Register
public class S3Controller implements Controller {

    @Override
    public void onError(WebContext ctx, HandledException error) {
        signalObjectError(ctx, HttpResponseStatus.BAD_REQUEST, error.getMessage());
    }

    @Part
    private Storage storage;

    @Part
    private APILog log;

    private Map<String, ReentrantLock> locks = Maps.newConcurrentMap();

    /*
     * Computes the expected hash for the given request.
     */
    private String computeHash(WebContext ctx, String pathPrefix) {
        try {
            Matcher aws4Header = AWS_AUTH4_PATTERN.matcher(ctx.getHeader("Authorization"));
            if (aws4Header.matches()) {
                return computeAWS4Hash(ctx, aws4Header);
            } else {
                return computeAWSLegacyHash(ctx, pathPrefix);
            }
        } catch (Throwable e) {
            throw Exceptions.handle(UserContext.LOG, e);
        }
    }

    /*
     * Computes the "classic" authentication hash.
     */
    private String computeAWSLegacyHash(WebContext ctx, String pathPrefix) throws Exception {
        StringBuilder stringToSign = new StringBuilder(ctx.getRequest().getMethod().name());
        stringToSign.append("\n");
        stringToSign.append(ctx.getHeaderValue("Content-MD5").asString(""));
        stringToSign.append("\n");
        stringToSign.append(ctx.getHeaderValue("Content-Type").asString(""));
        stringToSign.append("\n");
        stringToSign.append(ctx.get("Expires")
                               .asString(ctx.getHeaderValue("x-amz-date")
                                            .asString(ctx.getHeaderValue("Date").asString(""))));
        stringToSign.append("\n");

        List<String> headers = Lists.newArrayList();
        for (String name : ctx.getRequest().headers().names()) {
            if (name.toLowerCase().startsWith("x-amz-") && !"x-amz-date".equals(name.toLowerCase())) {
                StringBuilder headerBuilder = new StringBuilder(name.toLowerCase().trim());
                headerBuilder.append(":");
                headerBuilder.append(Strings.join(ctx.getRequest().headers().getAll(name), ",").trim());
                headers.add(headerBuilder.toString());
            }
        }
        Collections.sort(headers);
        for (String header : headers) {
            stringToSign.append(header);
            stringToSign.append("\n");
        }

        stringToSign.append(pathPrefix).append(ctx.getRequest().getUri().substring(3));

        SecretKeySpec keySpec = new SecretKeySpec(storage.getAwsSecretKey().getBytes(), "HmacSHA1");
        Mac mac = Mac.getInstance("HmacSHA1");
        mac.init(keySpec);
        byte[] result = mac.doFinal(stringToSign.toString().getBytes(Charsets.UTF_8.name()));
        return BaseEncoding.base64().encode(result);
    }

    /*
     * Computes the AWS Version 4 signing hash
     */
    private String computeAWS4Hash(WebContext ctx, Matcher aws4Header) throws Exception {
        StringBuilder canonicalRequest = new StringBuilder(ctx.getRequest().getMethod().name());
        canonicalRequest.append("\n");
        canonicalRequest.append(ctx.getRequestedURI());
        canonicalRequest.append("\n");
        canonicalRequest.append(ctx.getQueryString());
        canonicalRequest.append("\n");
        for (String name : aws4Header.group(4).split(";")) {
            canonicalRequest.append(name.trim());
            canonicalRequest.append(":");
            canonicalRequest.append(Strings.join(ctx.getRequest().headers().getAll(name), ",").trim());
            canonicalRequest.append("\n");
        }
        canonicalRequest.append("\n");
        canonicalRequest.append(aws4Header.group(4));
        canonicalRequest.append("\n");
        canonicalRequest.append(ctx.getHeader("x-amz-content-sha256"));

        StringBuilder stringToSign = new StringBuilder("AWS4-HMAC-SHA256\n");
        stringToSign.append(ctx.getHeader("x-amz-date"));
        stringToSign.append("\n");
        stringToSign.append(ctx.getHeader("x-amz-date").substring(0, 8));
        stringToSign.append("/");
        stringToSign.append(aws4Header.group(3));
        stringToSign.append("/s3/aws4_request\n");
        stringToSign.append(Hashing.sha256().hashString(canonicalRequest, Charsets.UTF_8).toString());

        byte[] dateKey = hmacSHA256(("AWS4" + storage.getAwsSecretKey()).getBytes(Charsets.UTF_8), aws4Header.group(2));
        byte[] dateRegionKey = hmacSHA256(dateKey, aws4Header.group(3));
        byte[] dateRegionServiceKey = hmacSHA256(dateRegionKey, "s3");
        byte[] signingKey = hmacSHA256(dateRegionServiceKey, "aws4_request");

        return BaseEncoding.base16().lowerCase().encode(hmacSHA256(signingKey, stringToSign.toString()));
    }

    private byte[] hmacSHA256(byte[] key, String value) throws Exception {
        SecretKeySpec keySpec = new SecretKeySpec(key, "HmacSHA256");
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(keySpec);
        return mac.doFinal(value.getBytes(Charsets.UTF_8));
    }

    private static final Pattern AWS_AUTH_PATTERN = Pattern.compile("AWS ([^:]+):(.*)");
    private static final Pattern AWS_AUTH4_PATTERN = Pattern.compile(
            "AWS4-HMAC-SHA256 Credential=([^/]+)/([^/]+)/([^/]+)/s3/aws4_request, SignedHeaders=([^,]+), Signature=(.+)");

    /*
     * Extracts the given hash from the given request. Returns null if no hash was given.
     */
    private String getAuthHash(WebContext ctx) {
        Value auth = ctx.getHeaderValue(HttpHeaders.Names.AUTHORIZATION);
        if (!auth.isFilled()) {
            return ctx.get("Signature").getString();
        }
        Matcher m = AWS_AUTH_PATTERN.matcher(auth.getString());
        if (m.matches()) {
            return m.group(2);
        }

        m = AWS_AUTH4_PATTERN.matcher(auth.getString());
        if (m.matches()) {
            return m.group(5);
        }

        return null;
    }

    /*
     * Writes an API error to the log
     */
    private void signalObjectError(WebContext ctx, HttpResponseStatus status, String message) {
        ctx.respondWith().error(status, message);
        log.log("OBJECT " + ctx.getRequest().getMethod().name(),
                message + " - " + ctx.getRequestedURI(),
                APILog.Result.ERROR,
                CallContext.getCurrent().getWatch());
    }

    /*
     * Writes an API success entry to the log
     */
    private void signalObjectSuccess(WebContext ctx) {
        log.log("OBJECT " + ctx.getRequest().getMethod().name(),
                ctx.getRequestedURI(),
                APILog.Result.OK,
                CallContext.getCurrent().getWatch());
    }

    /**
     * Dispatching method handling all object specific calls.
     *
     * @param ctx        the context describing the current request
     * @param bucketName name of the bucket which contains the object (must exist)
     * @param idList     name of the object ob interest
     * @throws Exception in case of IO errors and there like
     */
    @Routed("/s3/:1/**")
    public void object(WebContext ctx, String bucketName, List<String> idList) throws Exception {
        Bucket bucket = storage.getBucket(bucketName);
        if (!bucket.exists()) {
            if (storage.isAutocreateBuckets()) {
                bucket.create();
            } else {
                signalObjectError(ctx, HttpResponseStatus.NOT_FOUND, "Bucket does not exist");
                return;
            }
        }
        String id = idList.stream().collect(Collectors.joining("/")).replace('/', '_');
        if (Strings.isEmpty(id)) {
            // if it's a request to the bucket, it's usually a bucket create command.
            // As we allow bucket creation, thus send a positive response
            if (ctx.getRequest().getMethod() == HttpMethod.HEAD || ctx.getRequest().getMethod() == HttpMethod.GET) {
                signalObjectSuccess(ctx);
                ctx.respondWith().status(HttpResponseStatus.OK);
                return;
            }

            signalObjectError(ctx, HttpResponseStatus.NOT_FOUND, "Please provide an object id");
        }
        String hash = getAuthHash(ctx);
        if (hash != null) {
            String expectedHash = computeHash(ctx, "/s3");
            String alternativeHash = computeHash(ctx, "");
            if (!expectedHash.equals(hash) && !alternativeHash.equals(hash)) {
                ctx.respondWith()
                   .error(HttpResponseStatus.UNAUTHORIZED,
                          Strings.apply("Invalid Hash (Expected: %s, Found: %s)", expectedHash, hash));
                log.log("OBJECT " + ctx.getRequest().getMethod().name(),
                        ctx.getRequestedURI(),
                        APILog.Result.REJECTED,
                        CallContext.getCurrent().getWatch());
                return;
            }
        }
        if (bucket.isPrivate() && !ctx.get("noAuth").isFilled() && hash == null) {
            ctx.respondWith().error(HttpResponseStatus.UNAUTHORIZED, "Authentication required");
            log.log("OBJECT " + ctx.getRequest().getMethod().name(),
                    ctx.getRequestedURI(),
                    APILog.Result.REJECTED,
                    CallContext.getCurrent().getWatch());
            return;
        }
        if (ctx.getRequest().getMethod() == HttpMethod.GET) {
            getObject(ctx, bucket, id, true);
        } else if (ctx.getRequest().getMethod() == HttpMethod.PUT) {
            Value copy = ctx.getHeaderValue("x-amz-copy-source");
            if (copy.isFilled()) {
                copyObject(ctx, bucket, id, copy.asString());
            } else {
                putObject(ctx, bucket, id);
            }
        } else if (ctx.getRequest().getMethod() == HttpMethod.DELETE) {
            deleteObject(ctx, bucket, id);
        } else if (ctx.getRequest().getMethod() == HttpMethod.HEAD) {
            getObject(ctx, bucket, id, false);
        } else {
            throw new IllegalArgumentException(ctx.getRequest().getMethod().name());
        }
    }

    /**
     * Handles DELETE /bucket/id
     *
     * @param ctx    the context describing the current request
     * @param bucket the bucket containing the object to delete
     * @param id     name of the object to delete
     */

    private void deleteObject(WebContext ctx, Bucket bucket, String id) {
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
    private void putObject(WebContext ctx, Bucket bucket, String id) throws Exception {
        StoredObject object = bucket.getObject(id);
        InputStream inputStream = ctx.getContent();
        if (inputStream == null) {
            signalObjectError(ctx, HttpResponseStatus.BAD_REQUEST, "No content posted");
            return;
        }
        try {
            FileOutputStream out = new FileOutputStream(object.getFile());
            try {
                ByteStreams.copy(inputStream, out);
            } finally {
                out.close();
            }
        } finally {
            inputStream.close();
        }

        Map<String, String> properties = Maps.newTreeMap();
        for (String name : ctx.getRequest().headers().names()) {
            String nameLower = name.toLowerCase();
            if (nameLower.startsWith("x-amz-meta-") || nameLower.equals("content-md5") || nameLower.equals(
                    "content-type") || nameLower.equals("x-amz-acl")) {
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
        ctx.respondWith().addHeader(HttpHeaders.Names.ETAG, etag(hash)).status(HttpResponseStatus.OK);
        signalObjectSuccess(ctx);
    }

    private String etag(HashCode hash) {
        return "\"" + hash + "\"";
    }

    private DateTimeFormatter dateTimeFormatter =
            new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").toFormatter()
                    .withChronology(IsoChronology.INSTANCE).withZone(ZoneOffset.UTC);

    /**
     * Handles GET /bucket/id with an <tt>x-amz-copy-source</tt> header.
     *
     * @param ctx    the context describing the current request
     * @param bucket the bucket containing the object to use as destination
     * @param id     name of the object to use as destination
     */
    private void copyObject(WebContext ctx, Bucket bucket, String id, String copy) throws IOException {
        StoredObject object = bucket.getObject(id);
        /*
        if (!object.exists()) {
            signalObjectError(ctx, HttpResponseStatus.NOT_FOUND, "Object does not exist");
            return;
        }
        */
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
        XMLStructuredOutput structuredOutput = ctx.respondWith().addHeader(HttpHeaders.Names.ETAG, etag).xml();
        structuredOutput.beginOutput("CopyObjectResult");
        structuredOutput.beginObject("LastModified");
        structuredOutput.text(dateTimeFormatter.format(object.getLastModifiedInstant()));
        structuredOutput.endObject();
        structuredOutput.beginObject("ETag");
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
        if (sendFile) {
            response.file(object.getFile());
        } else {
            response.status(HttpResponseStatus.OK);
        }
        signalObjectSuccess(ctx);
    }
}
