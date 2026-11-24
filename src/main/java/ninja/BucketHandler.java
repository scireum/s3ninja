/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import ninja.errors.S3ErrorCode;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.xml.Attribute;
import sirius.kernel.xml.XMLStructuredOutput;
import sirius.web.http.Response;
import sirius.web.http.WebContext;

import java.time.Instant;
import java.util.List;
import java.util.Locale;

/**
 * Handles the S3 API calls which deal with buckets: listing, creating and deleting them, and listing their objects.
 */
@Register(classes = BucketHandler.class)
public class BucketHandler {

    private static final String ERROR_BUCKET_IS_NOT_EMPTY = "Bucket is not empty";

    private static final String ERROR_BUCKET_ALREADY_OWNED_BY_YOU = "Bucket already owned by you";

    /**
     * Names the region in which S3 answers a request to create a bucket which exists already with success.
     */
    private static final String LEGACY_REGION = "us-east-1";

    @Part
    private Storage storage;

    @Part
    private MultipartUploads multipartUploads;

    @Part
    private MultipartUploadHandler multipartUploadHandler;

    @Part
    private S3Responder responder;

    @Part
    private RequestAuthenticator authenticator;

    @Part
    private CredentialVerifier credentialVerifier;

    /**
     * GET a list of all buckets
     *
     * @param webContext the context describing the current request
     */
    void listBuckets(WebContext webContext) {
        HttpMethod method = webContext.getRequest().method();

        if (HttpMethod.GET.equals(method)) {
            List<Bucket> buckets = storage.getBuckets();
            Response response = webContext.respondWith();

            response.setHeader(S3Dispatcher.HTTP_HEADER_NAME_CONTENT_TYPE, S3Dispatcher.CONTENT_TYPE_XML);

            XMLStructuredOutput out = response.xml();
            out.beginOutput("ListAllMyBucketsResult",
                            Attribute.set("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/"));
            out.property("hint", "Goto: " + webContext.getBaseURL() + "/ui to visit the admin UI");
            responder.outputOwnerInfo(out, "Owner");

            out.beginObject("Buckets");
            for (Bucket bucket : buckets) {
                out.beginObject(S3Dispatcher.RESPONSE_BUCKET);
                out.property("Name", bucket.getName());
                out.property("CreationDate",
                             S3Dispatcher.ISO8601_INSTANT.format(Instant.ofEpochMilli(bucket.getFolder()
                                                                                            .lastModified())));
                out.endObject();
            }
            out.endObject();
            out.endOutput();
        } else {
            responder.signalMethodNotAllowed(webContext, null, null);
        }
    }

    /**
     * Dispatching method handling bucket-specific calls without content (HEAD, DELETE, GET, and PUT)
     *
     * @param webContext the context describing the current request
     * @param bucketName name of the bucket of interest
     */
    void bucket(WebContext webContext, String bucketName) {
        Bucket bucket = storage.getBucket(bucketName);

        if (!authenticator.objectCheckAuth(webContext, bucket, null)) {
            return;
        }

        HttpMethod method = webContext.getRequest().method();
        if (HttpMethod.HEAD.equals(method)) {
            headBucket(webContext, bucket);
        } else if (HttpMethod.GET.equals(method)) {
            getBucket(webContext, bucket);
        } else if (HttpMethod.DELETE.equals(method)) {
            deleteBucket(webContext, bucket);
        } else if (HttpMethod.POST.equals(method)) {
            postToBucket(webContext, bucket);
        } else if (HttpMethod.PUT.equals(method)) {
            createBucket(webContext, bucket);
        } else {
            responder.signalMethodNotAllowed(webContext, bucketName, null);
        }
    }

    /**
     * Handles HEAD /bucket
     */
    private void headBucket(WebContext webContext, Bucket bucket) {
        if (bucket.exists()) {
            responder.signalObjectSuccess(webContext);
            webContext.respondWith().status(HttpResponseStatus.OK);
        } else {
            signalNoSuchBucket(webContext, bucket);
        }
    }

    /**
     * Handles GET /bucket
     */
    private void getBucket(WebContext webContext, Bucket bucket) {
        if (bucket.exists()) {
            listObjects(webContext, bucket);
        } else {
            signalNoSuchBucket(webContext, bucket);
        }
    }

    /**
     * Handles DELETE /bucket
     * <p>
     * Only an empty bucket is deleted, along with the multipart uploads which are still open for it.
     */
    private void deleteBucket(WebContext webContext, Bucket bucket) {
        if (!bucket.exists()) {
            signalNoSuchBucket(webContext, bucket);
            return;
        }
        if (bucket.countObjects("") > 0) {
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        null,
                                        S3ErrorCode.BucketNotEmpty,
                                        ERROR_BUCKET_IS_NOT_EMPTY);
            return;
        }
        if (!bucket.delete()) {
            signalFileSystemError(webContext, bucket);
            return;
        }
        multipartUploads.abortAll(bucket.getName());

        responder.signalObjectSuccess(webContext);
        webContext.respondWith().status(HttpResponseStatus.NO_CONTENT);
    }

    /**
     * Handles POST /bucket which is no browser-based upload.
     * <p>
     * A form names the object to start a multipart upload for. Any other POST is refused, as by S3: with
     * <tt>NoSuchBucket</tt> if the bucket does not exist, and with <tt>PreconditionFailed</tt> otherwise.
     */
    private void postToBucket(WebContext webContext, Bucket bucket) {
        String contentType = webContext.getHeader(HttpHeaderNames.CONTENT_TYPE);
        boolean isForm = contentType != null && contentType.toLowerCase(Locale.ROOT).startsWith("multipart/form-data");
        if (!isForm && !bucket.exists()) {
            signalNoSuchBucket(webContext, bucket);
            return;
        }
        if (!isForm) {
            // As for S3, which creates buckets by PUT only.
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        null,
                                        S3ErrorCode.PreconditionFailed,
                                        "Bucket POST must be of the enclosure-type multipart/form-data");
            return;
        }

        if (!bucket.exists()) {
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        null,
                                        S3ErrorCode.InternalError,
                                        "Presigned POST not supported.");
            return;
        }

        String objectKey = extractKeyFromMultipartForm(webContext);
        if (!Strings.isFilled(objectKey)) {
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        null,
                                        S3ErrorCode.InvalidRequest,
                                        "Please provide an object key.");
            return;
        }
        multipartUploadHandler.startMultipartUpload(webContext, bucket, objectKey);
    }

    /**
     * Handles PUT /bucket
     * <p>
     * S3 refuses to create a bucket which its owner has created already, except in its oldest region. There it
     * answers with success for legacy compatibility, and resets the access control list of the bucket.
     */
    private void createBucket(WebContext webContext, Bucket bucket) {
        boolean recreatedInLegacyRegion = bucket.exists() && credentialVerifier.determineRegion(webContext)
                                                                               .filter(LEGACY_REGION::equals)
                                                                               .isPresent();
        if (bucket.exists() && !recreatedInLegacyRegion) {
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        null,
                                        S3ErrorCode.BucketAlreadyOwnedByYou,
                                        ERROR_BUCKET_ALREADY_OWNED_BY_YOU);
            return;
        }

        if (recreatedInLegacyRegion ? !bucket.makePrivate() : !bucket.create()) {
            signalFileSystemError(webContext, bucket);
            return;
        }

        // in order to allow creation of public buckets, we support a single canned access control list
        String cannedAccessControlList = webContext.getHeader("x-amz-acl");
        if (Strings.areEqual(cannedAccessControlList, "public-read-write") && !bucket.makePublic()) {
            signalFileSystemError(webContext, bucket);
            return;
        }

        responder.signalObjectSuccess(webContext);
        webContext.respondWith().status(HttpResponseStatus.OK);
    }

    private void signalNoSuchBucket(WebContext webContext, Bucket bucket) {
        responder.signalObjectError(webContext,
                                    bucket.getName(),
                                    null,
                                    S3ErrorCode.NoSuchBucket,
                                    S3Dispatcher.ERROR_BUCKET_DOES_NOT_EXIST);
    }

    private void signalFileSystemError(WebContext webContext, Bucket bucket) {
        responder.signalObjectError(webContext,
                                    bucket.getName(),
                                    null,
                                    S3ErrorCode.InternalError,
                                    S3Dispatcher.ERROR_FILE_SYSTEM_ACCESS);
    }

    private String extractKeyFromMultipartForm(WebContext webContext) {
        // First try the standard parameter (works for both form fields and query params)
        String key = webContext.getParameter("key");
        if (Strings.isFilled(key)) {
            return key;
        }

        // Try to get it from request parameter (different method for multipart)
        if (webContext.hasParameter("key")) {
            key = webContext.get("key").asString();
            if (Strings.isFilled(key)) {
                return key;
            }
        }

        return null;
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
        Response response = webContext.respondWith();
        response.setHeader(S3Dispatcher.HTTP_HEADER_NAME_CONTENT_TYPE, S3Dispatcher.CONTENT_TYPE_XML);
        ObjectListing.outputVersion1(bucket, webContext, response.xml());
    }

    private void listObjectsV2(WebContext webContext, Bucket bucket) {
        if (!ObjectListing.hasValidContinuationToken(webContext)) {
            responder.signalObjectError(webContext,
                                        bucket.getName(),
                                        null,
                                        S3ErrorCode.InvalidArgument,
                                        "The continuation token provided is incorrect");
            return;
        }

        Response response = webContext.respondWith();
        response.setHeader(S3Dispatcher.HTTP_HEADER_NAME_CONTENT_TYPE, S3Dispatcher.CONTENT_TYPE_XML);
        ObjectListing.outputVersion2(bucket, webContext, response.xml());
    }
}
