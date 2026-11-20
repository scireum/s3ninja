/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import ninja.errors.S3ErrorCode;
import ninja.errors.S3ErrorSynthesizer;
import ninja.queries.S3QueryProcessor;
import sirius.kernel.commons.Callback;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Value;
import sirius.kernel.di.GlobalContext;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Log;
import sirius.web.http.InputStreamHandler;
import sirius.web.http.WebContext;
import sirius.web.http.WebDispatcher;

import java.io.IOException;
import java.net.InetAddress;
import java.time.ZoneOffset;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;

/**
 * Handles S3 API Calls.
 */
@Register(classes = {WebDispatcher.class, S3Dispatcher.class})
public class S3Dispatcher implements WebDispatcher {

    /**
     * Names the parameter which addresses the multipart uploads of a bucket.
     */
    private static final String PARAMETER_UPLOADS = "uploads";

    /**
     * Names the parameter which states the number of a part of a multipart upload.
     */
    private static final String PARAMETER_PART_NUMBER = "partNumber";

    /**
     * Logger for S3 dispatcher operations including session tokens.
     */
    protected static final Log LOG = Log.get("s3dispatcher");

    private static final String UI_PATH = "ui";
    private static final String UI_PATH_PREFIX = "ui/";
    private static final String API_PATH = ".api/";
    private static final String API_PATH_PREFIX = ".api/";

    static final String HTTP_HEADER_NAME_ETAG = "ETag";
    static final String HTTP_HEADER_NAME_CONTENT_TYPE = "Content-Type";
    static final String CONTENT_TYPE_XML = "application/xml";
    static final String RESPONSE_BUCKET = "Bucket";
    static final String ERROR_BUCKET_DOES_NOT_EXIST = "Bucket does not exist";
    static final String ERROR_PRECONDITION_FAILED = "At least one of the pre-conditions you specified did not hold.";
    static final String ERROR_FILE_SYSTEM_ACCESS =
            "General problems accessing the file system — Permissions set correctly?";
    static final String AMAZON_HEADER_PREFIX = "x-amz-";

    /**
     * Names the parameters which refine a listing, and are therefore no sub-resource even if sent without a value.
     */
    private static final Set<String> LISTING_PARAMETERS = Set.of("prefix",
                                                                 "delimiter",
                                                                 "marker",
                                                                 "max-keys",
                                                                 "encoding-type",
                                                                 "start-after",
                                                                 "continuation-token",
                                                                 "fetch-owner",
                                                                 "key-marker",
                                                                 "upload-id-marker",
                                                                 "max-uploads");

    private static class S3Request {

        private String uri;

        private String bucket;

        private String key;

        private String query;
    }

    @Part
    private static GlobalContext globalContext;

    @Part
    private S3ErrorSynthesizer errorSynthesizer;

    @Part
    private S3Responder responder;

    @Part
    private RequestAuthenticator authenticator;

    @Part
    private BucketHandler bucketHandler;

    @Part
    private ObjectHandler objectHandler;

    @Part
    private PostObjectHandler postObjectHandler;

    @Part
    private MultipartUploadHandler multipartUploadHandler;

    /**
     * ISO 8601 date/time formatter.
     */
    public static final DateTimeFormatter ISO8601_INSTANT =
            new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
                                          .toFormatter()
                                          .withLocale(Locale.ENGLISH)
                                          .withChronology(IsoChronology.INSTANCE)
                                          .withZone(ZoneOffset.ofHours(0));

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
        } catch (Exception exception) {
            // reaching this point, we failed to resolve the local host name. tant pis.
            Exceptions.ignore(exception);
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
        if (request.uri.equals(API_PATH) || request.uri.startsWith(API_PATH_PREFIX)) {
            return null;
        }
        if (hasInvalidBucketName(request)) {
            // Answered by dispatch(...).
            return null;
        }
        if (Strings.isFilled(request.query) && !Strings.areEqual(request.query, PARAMETER_UPLOADS) && !Strings.areEqual(
                request.query,
                "delete")) {
            // Handled by dispatch(...) once the body has arrived.
            return null;
        }

        // Check for presigned POST to set content handler
        if (HttpMethod.POST.equals(webContext.getRequest().method())) {
            String contentType = webContext.getHeader(HttpHeaderNames.CONTENT_TYPE);
            if (contentType != null && contentType.toLowerCase(Locale.ROOT).startsWith("multipart/form-data")) {
                if (webContext.hasParameter("policy") || webContext.hasParameter("Policy")) {
                    InputStreamHandler handler = createInputStreamHandler(webContext);
                    webContext.setContentHandler(handler);
                }
            }
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
        return req -> {
            try {
                writeObject(req, request.bucket, request.key, handler);
            } catch (Exception exception) {
                answerUnexpectedError(req, exception);
            }
        };
    }

    private InputStreamHandler createInputStreamHandler(WebContext webContext) {
        if (aws4HashCalculator.supports(webContext)
            && HttpMethod.PUT.equals(webContext.getRequest().method())
            && webContext.getHeader("x-amz-decoded-content-length") != null) {
            // We have to pass the web context to the handler, as it is necessary to access the request
            return new SignedChunkHandler(webContext);
        } else {
            return new InputStreamHandler();
        }
    }

    @Override
    public DispatchDecision dispatch(WebContext webContext) throws Exception {
        S3Request request = parseRequest(webContext);

        if (request.uri.equals(UI_PATH)
            || request.uri.startsWith(UI_PATH_PREFIX)
            || request.uri.equals(API_PATH)
            || request.uri.startsWith(API_PATH_PREFIX)) {
            return DispatchDecision.CONTINUE;
        }

        if (hasInvalidBucketName(request)) {
            errorSynthesizer.synthesiseError(webContext,
                                             null,
                                             null,
                                             S3ErrorCode.InvalidBucketName,
                                             "The specified bucket is not valid.");
            return DispatchDecision.DONE;
        }

        try {
            return dispatchRequest(webContext, request);
        } catch (Exception exception) {
            answerUnexpectedError(webContext, exception);
            return DispatchDecision.DONE;
        }
    }

    private boolean hasInvalidBucketName(S3Request request) {
        return Strings.isFilled(request.bucket) && !Bucket.isValidName(request.bucket);
    }

    /**
     * Answers a request which failed for a reason no handler has foreseen.
     * <p>
     * Clients of S3 expect an error in XML, where the error page of the web server would only confuse them.
     */
    private void answerUnexpectedError(WebContext webContext, Exception exception) {
        String message = Exceptions.handle(LOG, exception).getMessage();
        if (!webContext.isResponseCommitted()) {
            errorSynthesizer.synthesiseError(webContext, null, null, S3ErrorCode.InternalError, message);
        }
    }

    private DispatchDecision dispatchRequest(WebContext webContext, S3Request request) throws IOException {
        if (isListMultipartUploads(webContext, request)) {
            multipartUploadHandler.listMultipartUploads(webContext, request.bucket);
            return DispatchDecision.DONE;
        }

        if (Strings.isFilled(request.query)) {
            forwardQueryToProcessor(webContext, request);
            return DispatchDecision.DONE;
        }

        if (Strings.isEmpty(request.bucket)) {
            if (authenticator.checkSignatureIfPresent(webContext)) {
                bucketHandler.listBuckets(webContext);
            }
            return DispatchDecision.DONE;
        }

        if (Strings.isEmpty(request.key)) {
            // Check for presigned POST
            if (HttpMethod.POST.equals(webContext.getRequest().method())) {
                String contentType = webContext.getHeader(HttpHeaderNames.CONTENT_TYPE);
                if (contentType != null && contentType.toLowerCase(Locale.ROOT).startsWith("multipart/form-data")) {
                    if (webContext.hasParameter("policy") || webContext.hasParameter("Policy")) {
                        request.query = "presigned-post";
                        forwardQueryToProcessor(webContext, request);
                        return DispatchDecision.DONE;
                    }
                }
            }
            bucketHandler.bucket(webContext, request.bucket);
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
     * Determines if the given request is <tt>GET /bucket?uploads</tt>.
     * <p>
     * This is checked via the parameter instead of {@link S3Request#query}, as clients are free to send
     * <tt>uploads</tt> after other parameters such as <tt>prefix</tt>.
     */
    private boolean isListMultipartUploads(WebContext webContext, S3Request request) {
        return HttpMethod.GET.equals(webContext.getRequest().method())
               && Strings.isFilled(request.bucket)
               && Strings.isEmpty(request.key)
               && webContext.hasParameter(PARAMETER_UPLOADS)
               && !isQueryForProcessor(request);
    }

    /**
     * Determines if the request leads with a parameter other than <tt>uploads</tt> for which a
     * {@link S3QueryProcessor} exists, such as <tt>acl</tt>, which then takes precedence.
     */
    private boolean isQueryForProcessor(S3Request request) {
        return Strings.isFilled(request.query)
               && !Strings.areEqual(request.query, PARAMETER_UPLOADS)
               && globalContext.getPart(request.query, S3QueryProcessor.class) != null;
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

        // We treat the first parameter without value as query string, unless it is one which merely refines a
        // listing. Clients send these without value as well, e.g. "?prefix=&max-keys=10".
        Iterator<String> parameterIterator = webContext.getParameterNames().iterator();
        String firstParameter = parameterIterator.hasNext() ? parameterIterator.next() : null;
        String query = Strings.isFilled(firstParameter)
                       && Strings.isEmpty(webContext.getParameter(firstParameter))
                       && !LISTING_PARAMETERS.contains(firstParameter) ? firstParameter : null;

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

        if (!authenticator.objectCheckAuth(webContext, bucket, request.key)) {
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
                                             S3ErrorCode.NotImplemented,
                                             String.format("Received unknown query '%s'.", request.query));
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
            objectHandler.getObject(webContext, bucket, key, false);
        } else if (HttpMethod.GET.equals(method)) {
            if (Strings.isFilled(uploadId)) {
                multipartUploadHandler.getPartList(webContext, bucket, key, uploadId);
            } else {
                objectHandler.getObject(webContext, bucket, key, true);
            }
        } else if (HttpMethod.DELETE.equals(method)) {
            if (Strings.isFilled(uploadId)) {
                multipartUploadHandler.abortMultipartUpload(webContext, bucket, key, uploadId);
            } else {
                objectHandler.deleteObject(webContext, bucket, key);
            }
        } else {
            responder.signalMethodNotAllowed(webContext, bucket.getName(), key);
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
            if (webContext.hasParameter(PARAMETER_PART_NUMBER) != Strings.isFilled(uploadId)) {
                // Without this check, a part lacking its upload ID would be stored as the object itself.
                ByteStreams.exhaust(in);
                responder.signalObjectError(webContext,
                                            bucket.getName(),
                                            key,
                                            S3ErrorCode.InvalidArgument,
                                            "Uploading a part requires both partNumber and uploadId.");
            } else if (webContext.hasParameter(PARAMETER_PART_NUMBER)) {
                // PUT /bucket/key?uploadId=X&partNumber=Y - Upload part in S3 Multipart Upload
                String partNumber = webContext.get(PARAMETER_PART_NUMBER).asString();
                if (copy.isFilled()) {
                    multipartUploadHandler.uploadPartCopy(webContext,
                                                          bucket,
                                                          key,
                                                          uploadId,
                                                          partNumber,
                                                          copy.asString(),
                                                          in);
                } else {
                    multipartUploadHandler.uploadPart(webContext, bucket, key, uploadId, partNumber, in);
                }
            } else if (copy.isFilled()) {
                objectHandler.copyObject(webContext, bucket, key, copy.asString());
            } else {
                objectHandler.putObject(webContext, bucket, key, in);
            }
        } else if (HttpMethod.POST.equals(method)) {
            // S3 Multipart Upload API endpoints (different from Browser-Based Presigned POST)
            if (webContext.hasParameter(PARAMETER_UPLOADS)) {
                // POST /bucket/key?uploads - Initiate Multipart Upload
                multipartUploadHandler.startMultipartUpload(webContext, bucket, key);
            } else if (Strings.isFilled(uploadId)) {
                // POST /bucket/key?uploadId=X - Complete Multipart Upload
                multipartUploadHandler.completeMultipartUpload(webContext, bucket, key, uploadId, in);
            } else {
                // POST /bucket/key - Policy-based POST upload (with key in URL)
                postObjectHandler.handlePostObject(webContext, bucket, key, in);
            }
        } else {
            responder.signalMethodNotAllowed(webContext, bucket.getName(), key);
        }
    }

    private boolean checkObjectRequest(WebContext webContext, Bucket bucket, String id) {
        if (Strings.isEmpty(id)) {
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        id,
                                        S3ErrorCode.NoSuchKey,
                                        "Please provide an object id.");
            return false;
        }
        if (!StoredObject.isStorableKey(id)) {
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        id,
                                        S3ErrorCode.KeyTooLongError,
                                        "Your key is too long");
            return false;
        }
        if (!authenticator.objectCheckAuth(webContext, bucket, id)) {
            return false;
        }

        if (!bucket.exists()) {
            if (storage.isAutocreateBuckets()) {
                if (!bucket.create()) {
                    responder.signalObjectError(webContext,
                                                bucket.getName(),
                                                id,
                                                S3ErrorCode.InternalError,
                                                ERROR_FILE_SYSTEM_ACCESS);
                    return false;
                }
            } else {
                responder.signalObjectError(webContext,
                                            bucket.getName(),
                                            id,
                                            S3ErrorCode.NoSuchBucket,
                                            ERROR_BUCKET_DOES_NOT_EXIST);
                return false;
            }
        }
        return true;
    }
}
