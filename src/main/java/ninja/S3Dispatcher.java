/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import ninja.errors.S3ErrorCode;
import ninja.errors.S3ErrorSynthesizer;
import ninja.queries.S3QueryProcessor;
import org.asynchttpclient.BoundRequestBuilder;
import sirius.kernel.async.CallContext;
import sirius.kernel.commons.Callback;
import sirius.kernel.commons.Hasher;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Value;
import sirius.kernel.di.GlobalContext;
import sirius.kernel.di.std.ConfigValue;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Counter;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Log;
import sirius.kernel.xml.Attribute;
import sirius.kernel.xml.Outcall;
import sirius.kernel.xml.XMLReader;
import sirius.kernel.xml.XMLStructuredOutput;
import sirius.web.http.InputStreamHandler;
import sirius.web.http.MimeHelper;
import sirius.web.http.Response;
import sirius.web.http.WebContext;
import sirius.web.http.WebDispatcher;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.regex.Matcher;

import static ninja.Aws4HashCalculator.AWS_AUTH4_PATTERN;
import static ninja.AwsHashCalculator.AWS_AUTH_PATTERN;

/**
 * Handles S3 API Calls.
 */
@Register
public class S3Dispatcher implements WebDispatcher {

    private static final String UI_PATH = "ui";
    private static final String UI_PATH_PREFIX = "ui/";

    private static final String TEMPORARY_PROPERTIES_FILENAME = "properties";

    private static final String HTTP_HEADER_NAME_ETAG = "ETag";
    private static final String HTTP_HEADER_NAME_CONTENT_TYPE = "Content-Type";
    private static final String CONTENT_TYPE_XML = "application/xml";
    private static final String RESPONSE_DISPLAY_NAME = "DisplayName";
    private static final String RESPONSE_BUCKET = "Bucket";
    private static final String ERROR_MULTIPART_UPLOAD_DOES_NOT_EXIST = "Multipart Upload does not exist";
    private static final String ERROR_BUCKET_DOES_NOT_EXIST = "Bucket does not exist";
    private static final String PATH_DELIMITER = "/";

    private static class S3Request {

        private String uri;

        private String bucket;

        private String key;

        private String query;
    }

    @Part
    private static GlobalContext globalContext;

    @Part
    private APILog log;

    @Part
    private AwsHashCalculator hashCalculator;

    @Part
    private S3ErrorSynthesizer errorSynthesizer;

    @ConfigValue("storage.multipartDir")
    private String multipartDir;

    @Part
    private AwsUpstream awsUpstream;

    private final Set<String> multipartUploads = Collections.synchronizedSet(new TreeSet<>());

    private final Counter uploadIdCounter = new Counter();

    /**
     * ISO 8601 date/time formatter.
     */
    public static final DateTimeFormatter ISO8601_INSTANT =
            new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
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

    private static final ImmutableSet<String> DOMAINS;

    static {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        builder.add(".localhost");
        builder.add(".127.0.0.1");

        try {
            InetAddress myself = InetAddress.getLocalHost();
            builder.add('.' + myself.getHostAddress());
            builder.add('.' + myself.getHostName());
            builder.add('.' + myself.getCanonicalHostName());
        } catch (Exception e) {
            // reaching this point, we failed to resolve the local host name. tant pis.
            Exceptions.ignore(e);
        }

        DOMAINS = builder.build();
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
    public Callback<WebContext> preparePreDispatch(WebContext webContext) {
        S3Request request = parseRequest(webContext);

        if (request.uri.equals(UI_PATH) || request.uri.startsWith(UI_PATH_PREFIX)) {
            return null;
        }

        if (Strings.isFilled(request.query)
            && !Strings.areEqual(request.query, "uploads")
            && !Strings.areEqual(request.query, "delete")) {
            forwardQueryToProcessor(webContext, request);
            return null;
        }

        if (Strings.isEmpty(request.bucket) || Strings.isEmpty(request.key)) {
            return null;
        }

        Bucket bucket = storage.getBucket(request.bucket);
        if (!bucket.exists() && !storage.isAutocreateBuckets()) {
            return null;
        }

        InputStreamHandler handler = createInputStreamHandler(webContext);
        webContext.setContentHandler(handler);
        return req -> writeObject(req, request.bucket, request.key, handler);
    }

    private InputStreamHandler createInputStreamHandler(WebContext webContext) {
        if (aws4HashCalculator.supports(webContext)
            && HttpMethod.PUT.equals(webContext.getRequest().method())
            && webContext.getHeader("x-amz-decoded-content-length") != null) {
            return new SignedChunkHandler();
        } else {
            return new InputStreamHandler();
        }
    }

    @Override
    public DispatchDecision dispatch(WebContext webContext) throws Exception {
        S3Request request = parseRequest(webContext);

        if (request.uri.equals(UI_PATH) || request.uri.startsWith(UI_PATH_PREFIX)) {
            return DispatchDecision.CONTINUE;
        }

        if (Strings.isFilled(request.query)) {
            forwardQueryToProcessor(webContext, request);
            return DispatchDecision.DONE;
        }

        if (Strings.isEmpty(request.bucket)) {
            listBuckets(webContext);
            return DispatchDecision.DONE;
        }

        if (Strings.isEmpty(request.key)) {
            bucket(webContext, request.bucket);
            return DispatchDecision.DONE;
        }

        Bucket bucket = storage.getBucket(request.bucket);
        if (!bucket.exists() && !storage.isAutocreateBuckets()) {
            return DispatchDecision.CONTINUE;
        }

        readObject(webContext, request.bucket, request.key);
        return DispatchDecision.DONE;
    }

    /**
     * Returns the effective URI.
     * <p>
     * As we have to support legacy URIs which have an <tt>/s3</tt> prefix, we cut this here, and
     * also the first "/" and only return the effective URI to process.
     *
     * @param uri the requested URI
     * @return the effective URI to process
     */
    public static String getEffectiveURI(String uri) {
        if (uri.startsWith("/s3")) {
            uri = uri.substring(3);
        }
        if (uri.startsWith("/")) {
            uri = uri.substring(1);
        }

        return uri;
    }

    /**
     * Parses a S3 request from the given HTTP request.
     *
     * @param webContext the HTTP request to parse.
     * @return a structured {@link S3Request}.
     */
    private static S3Request parseRequest(WebContext webContext) {
        String uri = getEffectiveURI(webContext.getRequestedURI());

        // we treat the first parameter without value as query string
        Iterator<String> parameterIterator = webContext.getParameterNames().iterator();
        String firstParameter = parameterIterator.hasNext() ? parameterIterator.next() : null;
        String query = Strings.isFilled(firstParameter) && Strings.isEmpty(webContext.getParameter(firstParameter)) ?
                       firstParameter :
                       null;

        // chop off potential port from host
        Tuple<String, String> hostAndPort = Strings.split(webContext.getHeader("Host"), ":");
        String host = hostAndPort.getFirst();

        // check whether the host contains a subdomain by matching against the list of local domains
        if (Strings.isFilled(host)) {
            for (String domain : DOMAINS) {
                int length = host.length() - domain.length();
                if (host.endsWith(domain) && length > 0) {
                    S3Request request = new S3Request();
                    request.bucket = host.substring(0, length);
                    request.key = uri;
                    request.uri = request.bucket + "/" + request.key;
                    request.query = query;
                    return request;
                }
            }
        }

        Tuple<String, String> bucketAndKey = Strings.split(uri, "/");

        S3Request request = new S3Request();
        request.bucket = bucketAndKey.getFirst();
        request.key = bucketAndKey.getSecond();
        request.uri = uri;
        request.query = query;
        return request;
    }

    private void forwardQueryToProcessor(WebContext webContext, S3Request request) {
        Bucket bucket = storage.getBucket(request.bucket);
        if (!bucket.exists()) {
            errorSynthesizer.synthesiseError(webContext,
                                             bucket.getName(),
                                             request.key,
                                             S3ErrorCode.NoSuchBucket,
                                             ERROR_BUCKET_DOES_NOT_EXIST);
            return;
        }

        S3QueryProcessor processor = globalContext.getPart(request.query, S3QueryProcessor.class);
        if (processor != null) {
            processor.processQuery(webContext, bucket, request.key, request.query);
        } else {
            Log.BACKGROUND.WARN("Received unknown query '%s'.", request.query);
            errorSynthesizer.synthesiseError(webContext,
                                             request.bucket,
                                             request.key,
                                             S3ErrorCode.InvalidRequest,
                                             String.format("Received unknown query '%s'.", request.query));
        }
    }

    /**
     * Extracts the given hash from the given request. Returns null if no hash was given.
     */
    private String getAuthHash(WebContext webContext) {
        Value authorizationHeaderValue = webContext.getHeaderValue(HttpHeaderNames.AUTHORIZATION);
        if (!authorizationHeaderValue.isFilled()) {
            return webContext.get("Signature").asString(webContext.get("X-Amz-Signature").asString());
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
    private void signalObjectError(WebContext webContext,
                                   String bucket,
                                   String key,
                                   S3ErrorCode errorCode,
                                   String message) {
        if (HttpMethod.HEAD.equals(webContext.getRequest().method())) {
            webContext.respondWith().status(errorCode.getHttpStatusCode());
        } else {
            errorSynthesizer.synthesiseError(webContext, bucket, key, errorCode, message);
        }
        log.log(webContext.getRequest().method().name(),
                message + " - " + webContext.getRequestedURI(),
                APILog.Result.ERROR,
                CallContext.getCurrent().getWatch());
    }

    /**
     * Writes an API success entry to the log
     */
    private void signalObjectSuccess(WebContext webContext) {
        log.log(webContext.getRequest().method().name(),
                webContext.getRequestedURI(),
                APILog.Result.OK,
                CallContext.getCurrent().getWatch());
    }

    /**
     * GET a list of all buckets
     *
     * @param webContext the context describing the current request
     */
    private void listBuckets(WebContext webContext) {
        HttpMethod method = webContext.getRequest().method();

        if (HttpMethod.GET.equals(method)) {
            List<Bucket> buckets = storage.getBuckets();
            Response response = webContext.respondWith();

            response.setHeader(HTTP_HEADER_NAME_CONTENT_TYPE, CONTENT_TYPE_XML);

            XMLStructuredOutput out = response.xml();
            out.beginOutput("ListAllMyBucketsResult",
                            Attribute.set("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/"));
            out.property("hint", "Goto: " + webContext.getBaseURL() + "/ui to visit the admin UI");
            outputOwnerInfo(out, "Owner");

            out.beginObject("Buckets");
            for (Bucket bucket : buckets) {
                out.beginObject(RESPONSE_BUCKET);
                out.property("Name", bucket.getName());
                out.property("CreationDate",
                             ISO8601_INSTANT.format(Instant.ofEpochMilli(bucket.getFolder().lastModified())));
                out.endObject();
            }
            out.endObject();
            out.endOutput();
        } else {
            throw new IllegalArgumentException(webContext.getRequest().method().name());
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
     * @param webContext the context describing the current request
     * @param bucketName name of the bucket of interest
     */
    private void bucket(WebContext webContext, String bucketName) {
        Bucket bucket = storage.getBucket(bucketName);

        if (!objectCheckAuth(webContext, bucket, null)) {
            return;
        }

        HttpMethod method = webContext.getRequest().method();

        if (HttpMethod.HEAD.equals(method)) {
            if (bucket.exists()) {
                signalObjectSuccess(webContext);
                webContext.respondWith().status(HttpResponseStatus.OK);
            } else {
                signalObjectError(webContext, bucketName, null, S3ErrorCode.NoSuchBucket, ERROR_BUCKET_DOES_NOT_EXIST);
            }
        } else if (HttpMethod.GET.equals(method)) {
            if (bucket.exists()) {
                listObjects(webContext, bucket);
            } else {
                signalObjectError(webContext, bucketName, null, S3ErrorCode.NoSuchBucket, ERROR_BUCKET_DOES_NOT_EXIST);
            }
        } else if (HttpMethod.DELETE.equals(method)) {
            if (!bucket.exists()) {
                signalObjectError(webContext, bucketName, null, S3ErrorCode.NoSuchBucket, ERROR_BUCKET_DOES_NOT_EXIST);
            } else {
                bucket.delete();
                signalObjectSuccess(webContext);
                webContext.respondWith().status(HttpResponseStatus.OK);
            }
        } else if (HttpMethod.PUT.equals(method)) {
            bucket.create();

            // in order to allow creation of public buckets, we support a single canned access control list
            String cannedAccessControlList = webContext.getHeader("x-amz-acl");
            if (Strings.areEqual(cannedAccessControlList, "public-read-write")) {
                bucket.makePublic();
            }

            signalObjectSuccess(webContext);
            webContext.respondWith().status(HttpResponseStatus.OK);
        } else {
            throw new IllegalArgumentException(webContext.getRequest().method().name());
        }
    }

    /**
     * Dispatching method handling all object specific calls which either read or delete the object but do not provide
     * any data.
     *
     * @param webContext the context describing the current request
     * @param bucketName the name of the bucket which contains the object (must exist)
     * @param key        the key of the object of interest
     * @throws IOException in case of IO errors and there like
     */
    private void readObject(WebContext webContext, String bucketName, String key) throws IOException {
        Bucket bucket = storage.getBucket(bucketName);
        String uploadId = webContext.get("uploadId").asString();

        if (!checkObjectRequest(webContext, bucket, key)) {
            return;
        }

        HttpMethod method = webContext.getRequest().method();
        if (HttpMethod.HEAD.equals(method)) {
            getObject(webContext, bucket, key, false);
        } else if (HttpMethod.GET.equals(method)) {
            if (Strings.isFilled(uploadId)) {
                getPartList(webContext, bucket, key, uploadId);
            } else {
                getObject(webContext, bucket, key, true);
            }
        } else if (HttpMethod.DELETE.equals(method)) {
            if (Strings.isFilled(uploadId)) {
                abortMultipartUpload(webContext, uploadId);
            } else {
                deleteObject(webContext, bucket, key);
            }
        } else {
            throw new IllegalArgumentException(webContext.getRequest().method().name());
        }
    }

    /**
     * Dispatching method handling all object specific calls which write / provide data.
     *
     * @param webContext the context describing the current request
     * @param bucketName the name of the bucket which contains the object (must exist)
     * @param key        the key of the object of interest
     * @param in         the data to process
     * @throws IOException in case of IO errors and there like
     */
    private void writeObject(WebContext webContext, String bucketName, String key, InputStreamHandler in)
            throws IOException {
        Bucket bucket = storage.getBucket(bucketName);
        String uploadId = webContext.get("uploadId").asString();

        if (!checkObjectRequest(webContext, bucket, key)) {
            return;
        }

        HttpMethod method = webContext.getRequest().method();
        if (HttpMethod.PUT.equals(method)) {
            Value copy = webContext.getHeaderValue("x-amz-copy-source");
            if (copy.isFilled()) {
                copyObject(webContext, bucket, key, copy.asString());
            } else if (webContext.hasParameter("partNumber") && Strings.isFilled(uploadId)) {
                multiObject(webContext, uploadId, webContext.get("partNumber").asString(), in);
            } else {
                putObject(webContext, bucket, key, in);
            }
        } else if (HttpMethod.POST.equals(method)) {
            if (webContext.hasParameter("uploads")) {
                startMultipartUpload(webContext, bucket, key);
            } else if (Strings.isFilled(uploadId)) {
                completeMultipartUpload(webContext, bucket, key, uploadId, in);
            }
        } else {
            throw new IllegalArgumentException(webContext.getRequest().method().name());
        }
    }

    private boolean checkObjectRequest(WebContext webContext, Bucket bucket, String id) {
        if (Strings.isEmpty(id)) {
            signalObjectError(webContext, bucket.getName(), id, S3ErrorCode.NoSuchKey, "Please provide an object id.");
            return false;
        }
        if (!objectCheckAuth(webContext, bucket, id)) {
            return false;
        }

        if (!bucket.exists()) {
            if (storage.isAutocreateBuckets()) {
                bucket.create();
            } else {
                signalObjectError(webContext,
                                  bucket.getName(),
                                  id,
                                  S3ErrorCode.NoSuchBucket,
                                  ERROR_BUCKET_DOES_NOT_EXIST);
                return false;
            }
        }
        return true;
    }

    private boolean objectCheckAuth(WebContext webContext, Bucket bucket, String key) {
        String hash = getAuthHash(webContext);
        if (Strings.isFilled(hash)) {
            String expectedHash = hashCalculator.computeHash(webContext, "");
            String alternativeHash = hashCalculator.computeHash(webContext, "/s3");
            if (!expectedHash.equals(hash) && !alternativeHash.equals(hash)) {
                errorSynthesizer.synthesiseError(webContext,
                                                 bucket.getName(),
                                                 key,
                                                 S3ErrorCode.SignatureDoesNotMatch,
                                                 Strings.apply(
                                                         "The computed request signature does not match the one provided. Check login credentials. (Expected: %s, Found: %s)",
                                                         expectedHash,
                                                         hash));
                log.log(webContext.getRequest().method().name(),
                        webContext.getRequestedURI(),
                        APILog.Result.REJECTED,
                        CallContext.getCurrent().getWatch());
                return false;
            }
        }
        if (bucket.isPrivate() && !webContext.get("noAuth").isFilled() && Strings.isEmpty(hash)) {
            errorSynthesizer.synthesiseError(webContext,
                                             bucket.getName(),
                                             key,
                                             S3ErrorCode.AccessDenied,
                                             "Authentication required");
            log.log(webContext.getRequest().method().name(),
                    webContext.getRequestedURI(),
                    APILog.Result.REJECTED,
                    CallContext.getCurrent().getWatch());
            return false;
        }

        return true;
    }

    /**
     * Handles {@code GET /bucket} requests as triggered by
     * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html">{@code ListObjects}</a>
     * and <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html">{@code ListObjectsV2}</a>
     * calls.
     *
     * @param webContext the context describing the current request
     * @param bucket     the bucket of which the contents should be listed
     */
    private void listObjects(WebContext webContext, Bucket bucket) {
        if (webContext.get("list-type").asInt(1) == 2) {
            listObjectsV2(webContext, bucket);
        } else {
            listObjectsV1(webContext, bucket);
        }
    }

    private void listObjectsV1(WebContext webContext, Bucket bucket) {
        int maxKeys = webContext.get("max-keys").asInt(1000);
        String marker = webContext.get("marker").asString();
        String prefix = webContext.get("prefix").asString();

        Response response = webContext.respondWith();
        response.setHeader(HTTP_HEADER_NAME_CONTENT_TYPE, CONTENT_TYPE_XML);

        bucket.outputObjectsV1(response.xml(), maxKeys, marker, prefix);
    }

    private void listObjectsV2(WebContext webContext, Bucket bucket) {
        int maxKeys = webContext.get("max-keys").asInt(1000);
        String marker = webContext.get("start-after").asString();
        String prefix = webContext.get("prefix").asString();

        Response response = webContext.respondWith();
        response.setHeader(HTTP_HEADER_NAME_CONTENT_TYPE, CONTENT_TYPE_XML);

        bucket.outputObjectsV2(response.xml(), maxKeys, marker, prefix);
    }

    /**
     * Handles DELETE /bucket/id
     *
     * @param webContext the context describing the current request
     * @param bucket     the bucket containing the object to delete
     * @param id         name of the object to delete
     */
    private void deleteObject(final WebContext webContext, final Bucket bucket, final String id) {
        StoredObject object = bucket.getObject(id);
        object.delete();

        // If it exists online, we mark it locally as "deleted"
        if (awsUpstream.isConfigured() && awsUpstream.fetchClient().doesObjectExist(bucket.getName(), id)) {
            try {
                object.markDeleted();
            } catch (IOException ignored) {
                signalObjectError(webContext,
                                  bucket.getName(),
                                  id,
                                  S3ErrorCode.InternalError,
                                  Strings.apply("Error while marking file as deleted"));
                return;
            }
        }

        webContext.respondWith().status(HttpResponseStatus.NO_CONTENT);
        signalObjectSuccess(webContext);
    }

    /**
     * Handles PUT /bucket/id
     *
     * @param webContext the context describing the current request
     * @param bucket     the bucket containing the object to upload
     * @param id         name of the object to upload
     */
    private void putObject(WebContext webContext, Bucket bucket, String id, InputStreamHandler inputStream)
            throws IOException {
        StoredObject object = bucket.getObject(id);
        if (inputStream == null) {
            signalObjectError(webContext, bucket.getName(), id, S3ErrorCode.IncompleteBody, "No content posted");
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
            signalObjectError(webContext,
                              bucket.getName(),
                              id,
                              S3ErrorCode.BadDigest,
                              Strings.apply("Invalid MD5 checksum (Input: %s, Expected: %s)", contentMd5, md5));
            return;
        }
        String etag = BaseEncoding.base16().encode(hash).toLowerCase();
        properties.put(HTTP_HEADER_NAME_ETAG, etag);
        object.setProperties(properties);

        Response response = webContext.respondWith();
        response.addHeader(HTTP_HEADER_NAME_ETAG, etag(etag)).status(HttpResponseStatus.OK);
        response.addHeader(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS, HTTP_HEADER_NAME_ETAG);
        signalObjectSuccess(webContext);
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

    private String etag(String etag) {
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
    private void copyObject(WebContext webContext, Bucket bucket, String key, String sourcePath) throws IOException {
        if (Strings.isEmpty(sourcePath) || !sourcePath.contains(PATH_DELIMITER)) {
            signalObjectError(webContext,
                              null,
                              null,
                              S3ErrorCode.InvalidRequest,
                              String.format("Source '%s' must contain '/'", sourcePath));
            return;
        }

        // parse the path of the source object
        sourcePath = Strings.urlDecode(sourcePath);
        int sourceBucketNameStart = sourcePath.startsWith(PATH_DELIMITER) ? PATH_DELIMITER.length() : 0;
        String sourceBucketName =
                sourcePath.substring(sourceBucketNameStart, sourcePath.indexOf(PATH_DELIMITER, sourceBucketNameStart));
        String sourceKey = sourcePath.substring(sourcePath.indexOf(PATH_DELIMITER, sourceBucketNameStart) + 1);

        Bucket sourceBucket = storage.getBucket(sourceBucketName);
        if (!sourceBucket.exists()) {
            signalObjectError(webContext,
                              sourceBucketName,
                              sourceKey,
                              S3ErrorCode.NoSuchBucket,
                              String.format("Source bucket '%s' does not exist", sourceBucketName));
            return;
        }

        StoredObject sourceObject = sourceBucket.getObject(sourceKey);
        if (!sourceObject.exists()) {
            signalObjectError(webContext,
                              sourceBucketName,
                              sourceKey,
                              S3ErrorCode.NoSuchKey,
                              String.format("Source object '%s/%s' does not exist", sourceBucketName, sourceKey));
            return;
        }

        StoredObject object = bucket.getObject(key);
        Files.copy(sourceObject.getFile(), object.getFile());
        if (sourceObject.getPropertiesFile().exists()) {
            Files.copy(sourceObject.getPropertiesFile(), object.getPropertiesFile());
        }

        String etag = BaseEncoding.base16().encode(Hasher.md5().hashFile(object.getFile()).toHash()).toLowerCase();

        XMLStructuredOutput structuredOutput =
                webContext.respondWith().addHeader(HTTP_HEADER_NAME_ETAG, etag(etag)).xml();
        structuredOutput.beginOutput("CopyObjectResult");
        structuredOutput.beginObject("LastModified");
        structuredOutput.text(ISO8601_INSTANT.format(object.getLastModifiedInstant()));
        structuredOutput.endObject();
        structuredOutput.beginObject(HTTP_HEADER_NAME_ETAG);
        structuredOutput.text(etag(etag));
        structuredOutput.endObject();
        structuredOutput.endOutput();
        signalObjectSuccess(webContext);
    }

    /**
     * Handles GET /bucket/id
     *
     * @param webContext the context describing the current request
     * @param bucket     the bucket containing the object to download
     * @param id         name of the object to use as download
     */
    private void getObject(WebContext webContext, Bucket bucket, String id, boolean sendFile) throws IOException {
        StoredObject object = bucket.getObject(id);
        if (!object.exists() && !object.isMarkedDeleted() && awsUpstream.isConfigured()) {
            URL fetchURL = awsUpstream.generateGetObjectURL(bucket, object, sendFile);
            Consumer<BoundRequestBuilder> requestTuner =
                    requestBuilder -> requestBuilder.setMethod(sendFile ? "GET" : "HEAD");
            webContext.enableTiming(null).respondWith().tunnel(fetchURL.toString(), requestTuner, null, null);
            return;
        }

        if (!object.exists()) {
            signalObjectError(webContext, bucket.getName(), id, S3ErrorCode.NoSuchKey, "Object does not exist");
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
        String etag = properties.get(HTTP_HEADER_NAME_ETAG);
        if (Strings.isEmpty(etag)) {
            etag = BaseEncoding.base16().encode(Hasher.md5().hashFile(object.getFile()).toHash()).toLowerCase();
            properties.put(HTTP_HEADER_NAME_ETAG, etag);
            object.setProperties(properties);
        }
        response.addHeader(HTTP_HEADER_NAME_ETAG, etag(etag.toLowerCase()));
        response.addHeader(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS, HTTP_HEADER_NAME_ETAG);

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
        signalObjectSuccess(webContext);
    }

    /**
     * Handles POST /bucket/id?uploads
     *
     * @param webContext the context describing the current request
     * @param bucket     the bucket containing the object to upload
     * @param id         name of the object to upload
     */
    private void startMultipartUpload(WebContext webContext, Bucket bucket, String id) {
        Response response = webContext.respondWith();

        Map<String, String> properties = Maps.newTreeMap();
        for (String name : webContext.getRequest().headers().names()) {
            String nameLower = name.toLowerCase();
            if (nameLower.startsWith("x-amz-") || "content-md5".equals(nameLower) || "content-type".equals(nameLower)) {
                properties.put(name, webContext.getHeader(name));
                response.addHeader(name, webContext.getHeader(name));
            }
        }
        response.setHeader(HTTP_HEADER_NAME_CONTENT_TYPE, CONTENT_TYPE_XML);

        String uploadId = String.valueOf(uploadIdCounter.inc());
        multipartUploads.add(uploadId);

        getUploadDir(uploadId).mkdirs();

        storePropertiesInUploadDir(properties, uploadId);

        XMLStructuredOutput out = response.xml();
        out.beginOutput("InitiateMultipartUploadResult");
        out.property(RESPONSE_BUCKET, bucket.getName());
        out.property("Key", id);
        out.property("UploadId", uploadId);
        out.endOutput();
    }

    private void storePropertiesInUploadDir(Map<String, String> properties, String uploadId) {
        Properties props = new Properties();
        properties.forEach(props::setProperty);
        try (FileOutputStream propsOut = new FileOutputStream(new File(getUploadDir(uploadId),
                                                                       TEMPORARY_PROPERTIES_FILENAME))) {
            props.store(propsOut, "");
        } catch (IOException e) {
            Exceptions.handle(e);
        }
    }

    /**
     * Handles PUT /bucket/id?uploadId=X&partNumber=Y
     *
     * @param webContext the context describing the current request
     * @param uploadId   the multipart upload this part belongs to
     * @param partNumber the number of this part in the complete upload
     * @param part       input stream with the content of this part
     */
    private void multiObject(WebContext webContext, String uploadId, String partNumber, InputStreamHandler part) {
        if (!multipartUploads.contains(uploadId)) {
            errorSynthesizer.synthesiseError(webContext,
                                             null,
                                             null,
                                             S3ErrorCode.NoSuchUpload,
                                             ERROR_MULTIPART_UPLOAD_DOES_NOT_EXIST);
            return;
        }

        try {
            File partFile = new File(getUploadDir(uploadId), partNumber);
            partFile.deleteOnExit();
            Files.touch(partFile);

            try (FileOutputStream out = new FileOutputStream(partFile)) {
                ByteStreams.copy(part, out);
            }
            part.close();

            String etag = BaseEncoding.base16().encode(Hasher.md5().hashFile(partFile).toHash()).toLowerCase();
            webContext.respondWith()
                      .setHeader(HTTP_HEADER_NAME_ETAG, etag)
                      .addHeader(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS, HTTP_HEADER_NAME_ETAG)
                      .status(HttpResponseStatus.OK);
        } catch (IOException e) {
            errorSynthesizer.synthesiseError(webContext,
                                             null,
                                             null,
                                             S3ErrorCode.InternalError,
                                             Exceptions.handle(e).getMessage());
        }
    }

    /**
     * Handles POST /bucket/id?uploadId=X
     *
     * @param webContext the context describing the current request
     * @param bucket     the bucket containing the object to upload
     * @param id         name of the object to upload
     * @param uploadId   the multipart upload that should be completed
     * @param in         input stream with xml listing uploaded parts
     */
    private void completeMultipartUpload(WebContext webContext,
                                         Bucket bucket,
                                         String id,
                                         final String uploadId,
                                         InputStreamHandler in) {
        if (!multipartUploads.remove(uploadId)) {
            errorSynthesizer.synthesiseError(webContext,
                                             null,
                                             null,
                                             S3ErrorCode.NoSuchUpload,
                                             ERROR_MULTIPART_UPLOAD_DOES_NOT_EXIST);
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
                                      .toList());

        file.deleteOnExit();
        if (!file.exists()) {
            errorSynthesizer.synthesiseError(webContext,
                                             null,
                                             null,
                                             S3ErrorCode.NoSuchUpload,
                                             "Multipart File does not exist");
            return;
        }
        try {
            StoredObject object = bucket.getObject(id);
            Files.move(file, object.getFile());

            commitPropertiesFromUploadDir(uploadId, object);

            delete(getUploadDir(uploadId));

            String etag = Hasher.md5().hashFile(object.getFile()).toHexString();

            // update ETAG of the underlying object
            Map<String, String> properties = object.getProperties();
            properties.put(HTTP_HEADER_NAME_ETAG, etag);
            object.setProperties(properties);

            XMLStructuredOutput out = webContext.respondWith().xml();
            out.beginOutput("CompleteMultipartUploadResult");
            out.property("Location", "");
            out.property(RESPONSE_BUCKET, bucket.getName());
            out.property("Key", id);
            out.property(HTTP_HEADER_NAME_ETAG, etag);
            out.endOutput();
        } catch (IOException e) {
            Exceptions.ignore(e);
            errorSynthesizer.synthesiseError(webContext,
                                             null,
                                             null,
                                             S3ErrorCode.InternalError,
                                             "Could not build response");
        }
    }

    private void commitPropertiesFromUploadDir(String uploadId, StoredObject object) throws IOException {
        File propsFile = new File(getUploadDir(uploadId), TEMPORARY_PROPERTIES_FILENAME);
        if (propsFile.exists()) {
            Files.move(propsFile, object.getPropertiesFile());
        }
    }

    private File getUploadDir(String uploadId) {
        return new File(multipartDir + PATH_DELIMITER + uploadId);
    }

    private File combineParts(String id, String uploadId, List<File> parts) {
        File file = new File(getUploadDir(uploadId), StoredObject.encodeKey(id));

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
     * @param webContext the context describing the current request
     * @param uploadId   the multipart upload that should be cancelled
     */
    private void abortMultipartUpload(WebContext webContext, String uploadId) {
        multipartUploads.remove(uploadId);
        webContext.respondWith().status(HttpResponseStatus.OK);
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
     * @param webContext the context describing the current request
     * @param bucket     the bucket containing the object to download
     * @param id         name of the object to use as download
     */
    private void getPartList(WebContext webContext, Bucket bucket, String id, String uploadId) {
        if (!multipartUploads.contains(uploadId)) {
            errorSynthesizer.synthesiseError(webContext,
                                             null,
                                             null,
                                             S3ErrorCode.NoSuchUpload,
                                             ERROR_MULTIPART_UPLOAD_DOES_NOT_EXIST);
            return;
        }

        Response response = webContext.respondWith();

        response.setHeader(HTTP_HEADER_NAME_CONTENT_TYPE, CONTENT_TYPE_XML);

        XMLStructuredOutput out = response.xml();
        out.beginOutput("ListPartsResult");
        out.property(RESPONSE_BUCKET, bucket.getName());
        out.property("Key", id);
        out.property("UploadId", uploadId);

        outputOwnerInfo(out, "Initiator");
        outputOwnerInfo(out, "Owner");

        File uploadDir = getUploadDir(uploadId);
        int marker = webContext.get("part-number-marker").asInt(0);
        int maxParts = webContext.get("max-parts").asInt(0);

        FileFilter filter = file -> !Strings.areEqual(file.getName(), TEMPORARY_PROPERTIES_FILENAME);
        File[] parts = Objects.requireNonNull(uploadDir.listFiles(filter));

        out.property("StorageClass", "STANDARD");
        out.property("PartNumberMarker", marker);
        if ((marker + maxParts) < parts.length) {
            out.property("NextPartNumberMarker", marker + maxParts + 1);
        }

        if (Strings.isFilled(maxParts)) {
            out.property("MaxParts", maxParts);
        }

        boolean truncated = 0 < maxParts && maxParts < parts.length;
        out.property("IsTruncated", truncated);

        for (File part : parts) {
            out.beginObject("Part");
            out.property("PartNumber", part.getName());
            out.property("LastModified", ISO8601_INSTANT.format(Instant.ofEpochMilli(part.lastModified())));
            out.property(HTTP_HEADER_NAME_ETAG, Hasher.md5().hashFile(part).toHexString());
            out.property("Size", part.length());
            out.endObject();
        }

        out.endOutput();
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
