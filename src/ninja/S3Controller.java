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
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.multipart.Attribute;
import sirius.kernel.async.CallContext;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;
import sirius.web.controller.Controller;
import sirius.web.controller.Routed;
import sirius.web.controller.UserContext;
import sirius.web.http.Response;
import sirius.web.http.WebContext;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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


    /*
     * Computes the expected hash for the given request.
     */
    private String computeHash(WebContext ctx) {
        try {
            StringBuilder stringToSign = new StringBuilder(ctx.getRequest().getMethod().getName());
            stringToSign.append("\n");
            stringToSign.append(ctx.getHeaderValue("Content-MD5").asString(""));
            stringToSign.append("\n");
            stringToSign.append(ctx.getHeaderValue("Content-Type").asString(""));
            stringToSign.append("\n");
            stringToSign.append(ctx.getHeaderValue("x-amz-date").asString(ctx.getHeaderValue("Date").asString("")));
            stringToSign.append("\n");

            List<String> headers = Lists.newArrayList();
            for (String name : ctx.getRequest().getHeaderNames()) {
                if (name.toLowerCase().startsWith("x-amz-") && !"x-amz-date".equals(name.toLowerCase())) {
                    StringBuilder headerBuilder = new StringBuilder(name.toLowerCase().trim());
                    headerBuilder.append(":");
                    headerBuilder.append(Strings.join(ctx.getRequest().getHeaders(name), ",").trim());
                    headers.add(headerBuilder.toString());
                }
            }
            Collections.sort(headers);
            for (String header : headers) {
                stringToSign.append(header);
                stringToSign.append("\n");
            }

            stringToSign.append(ctx.getRequestedURI().substring(3));

            SecretKeySpec keySpec = new SecretKeySpec(storage.getAwsSecretKey().getBytes(), "HmacSHA1");

            Mac mac = Mac.getInstance("HmacSHA1");
            mac.init(keySpec);
            byte[] result = mac.doFinal(stringToSign.toString().getBytes(Charsets.UTF_8.name()));
            return BaseEncoding.base64().encode(result);
        } catch (Throwable e) {
            throw Exceptions.handle(UserContext.LOG, e);
        }
    }

    private static final Pattern AWS_AUTH_PATTERN = Pattern.compile("AWS ([^:]+):(.*)");

    /*
     * Extracts the given hash from the given request. Returns null if no hash was given.
     */
    private String getAuthHash(WebContext ctx) {
        Value auth = ctx.getHeaderValue(HttpHeaders.Names.AUTHORIZATION);
        if (!auth.isFilled()) {
            return null;
        }
        Matcher m = AWS_AUTH_PATTERN.matcher(auth.getString());
        if (m.matches()) {
            return m.group(2);
        }

        return null;
    }

    /*
     * Writes an API error to the log
     */
    private void signalObjectError(WebContext ctx, HttpResponseStatus status, String message) {
        ctx.respondWith().error(status, message);
        log.log("OBJECT " + ctx.getRequest().getMethod().getName(),
                ctx.getRequestedURI(),
                APILog.Result.ERROR,
                CallContext.getCurrent().getWatch());
    }

    /*
     * Writes an API success entry to the log
     */
    private void signalObjectSuccess(WebContext ctx) {
        log.log("OBJECT " + ctx.getRequest().getMethod().getName(),
                ctx.getRequestedURI(),
                APILog.Result.OK,
                CallContext.getCurrent().getWatch());
    }

    /**
     * Dispatching method handling all object specific calls.
     *
     * @param ctx        the context describing the current request
     * @param bucketName name of the bucket which contains the object (must exist)
     * @param id         name of the object ob interest
     * @throws Exception in case of IO errors and there like
     */
    @Routed("/s3/:1/:2")
    public void object(WebContext ctx, String bucketName, String id) throws Exception {
        Bucket bucket = storage.getBucket(bucketName);
        if (!bucket.exists()) {
            signalObjectError(ctx, HttpResponseStatus.NOT_FOUND, "Bucket does not exist");
            return;
        }
        String hash = getAuthHash(ctx);
        if (hash != null) {
            if (!computeHash(ctx).equals(hash)) {
                ctx.respondWith().error(HttpResponseStatus.UNAUTHORIZED, "Invalid Hash");
                log.log("OBJECT " + ctx.getRequest().getMethod().getName(),
                        ctx.getRequestedURI(),
                        APILog.Result.REJECTED,
                        CallContext.getCurrent().getWatch());
                return;
            }
        }
        if (bucket.isPrivate() && !ctx.get("noAuth").isFilled() && hash == null) {
            ctx.respondWith().error(HttpResponseStatus.UNAUTHORIZED, "Authentication required");
            log.log("OBJECT " + ctx.getRequest().getMethod().getName(),
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
        Attribute attr = ctx.getContent();
        long size = attr.getChannelBuffer().readableBytes();
        ChannelBufferInputStream inputStream = new ChannelBufferInputStream(attr.getChannelBuffer());
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
        for (String name : ctx.getRequest().getHeaderNames()) {
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
                signalObjectError(ctx, HttpResponseStatus.BAD_REQUEST, "Invalid MD5 checksum");
                return;
            }
        }

        object.storeProperties(properties);
        String etag = hash.toString();
        ctx.respondWith().addHeader(HttpHeaders.Names.ETAG, etag).status(HttpResponseStatus.OK);
        signalObjectSuccess(ctx);
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
        if (!object.exists()) {
            signalObjectError(ctx, HttpResponseStatus.NOT_FOUND, "Object does not exist");
            return;
        }
        if (!copy.contains("/")) {
            signalObjectError(ctx, HttpResponseStatus.BAD_REQUEST, "Source must contain '/'");
            return;
        }
        String srcBucketName = copy.substring(0, copy.lastIndexOf("/"));
        String srcId = copy.substring(copy.lastIndexOf("/"));
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
        ctx.respondWith().status(HttpResponseStatus.OK);
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
