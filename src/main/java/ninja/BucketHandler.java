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
            if (bucket.exists()) {
                responder.signalObjectSuccess(webContext);
                webContext.respondWith().status(HttpResponseStatus.OK);
            } else {
                responder.signalObjectError(webContext,
                                            bucketName,
                                            null,
                                            S3ErrorCode.NoSuchBucket,
                                            S3Dispatcher.ERROR_BUCKET_DOES_NOT_EXIST);
            }
        } else if (HttpMethod.GET.equals(method)) {
            if (bucket.exists()) {
                listObjects(webContext, bucket);
            } else {
                responder.signalObjectError(webContext,
                                            bucketName,
                                            null,
                                            S3ErrorCode.NoSuchBucket,
                                            S3Dispatcher.ERROR_BUCKET_DOES_NOT_EXIST);
            }
        } else if (HttpMethod.DELETE.equals(method)) {
            if (!bucket.exists()) {
                responder.signalObjectError(webContext,
                                            bucketName,
                                            null,
                                            S3ErrorCode.NoSuchBucket,
                                            S3Dispatcher.ERROR_BUCKET_DOES_NOT_EXIST);
            } else {
                if (bucket.countObjects("") > 0) {
                    responder.signalObjectError(webContext,
                                                bucketName,
                                                null,
                                                S3ErrorCode.BucketNotEmpty,
                                                ERROR_BUCKET_IS_NOT_EMPTY);
                    return;
                }

                if (!bucket.delete()) {
                    responder.signalObjectError(webContext,
                                                bucketName,
                                                null,
                                                S3ErrorCode.InternalError,
                                                S3Dispatcher.ERROR_FILE_SYSTEM_ACCESS);
                    return;
                }
                multipartUploads.abortAll(bucketName);

                responder.signalObjectSuccess(webContext);
                webContext.respondWith().status(HttpResponseStatus.NO_CONTENT);
            }
        } else if (HttpMethod.POST.equals(method)) {
            String contentType = webContext.getHeader(HttpHeaderNames.CONTENT_TYPE);
            if (contentType != null && contentType.toLowerCase(Locale.ROOT).startsWith("multipart/form-data")) {
                // Start multipart upload
                if (bucket.exists()) {
                    String objectKey = extractKeyFromMultipartForm(webContext);
                    if (!Strings.isFilled(objectKey)) {
                        responder.signalObjectError(webContext,
                                                    bucketName,
                                                    null,
                                                    S3ErrorCode.InvalidRequest,
                                                    "Please provide an object key.");
                        return;
                    }
                    multipartUploadHandler.startMultipartUpload(webContext, bucket, objectKey);
                } else {
                    responder.signalObjectError(webContext,
                                                bucketName,
                                                null,
                                                S3ErrorCode.InternalError,
                                                "Presigned POST not supported.");
                }
            } else {
                // Traditional bucket POST operations (creating bucket)
                if (bucket.exists()) {
                    responder.signalObjectError(webContext,
                                                bucketName,
                                                null,
                                                S3ErrorCode.BucketAlreadyOwnedByYou,
                                                ERROR_BUCKET_ALREADY_OWNED_BY_YOU);
                }
            }
        } else if (HttpMethod.PUT.equals(method)) {
            if (bucket.exists()) {
                responder.signalObjectError(webContext,
                                            bucketName,
                                            null,
                                            S3ErrorCode.BucketAlreadyOwnedByYou,
                                            ERROR_BUCKET_ALREADY_OWNED_BY_YOU);
                return;
            }

            if (!bucket.create()) {
                responder.signalObjectError(webContext,
                                            bucketName,
                                            null,
                                            S3ErrorCode.InternalError,
                                            S3Dispatcher.ERROR_FILE_SYSTEM_ACCESS);
                return;
            }

            // in order to allow creation of public buckets, we support a single canned access control list
            String cannedAccessControlList = webContext.getHeader("x-amz-acl");
            if (Strings.areEqual(cannedAccessControlList, "public-read-write") && !bucket.makePublic()) {
                responder.signalObjectError(webContext,
                                            bucketName,
                                            null,
                                            S3ErrorCode.InternalError,
                                            S3Dispatcher.ERROR_FILE_SYSTEM_ACCESS);
                return;
            }

            responder.signalObjectSuccess(webContext);
            webContext.respondWith().status(HttpResponseStatus.OK);
        } else {
            responder.signalMethodNotAllowed(webContext, bucketName, null);
        }
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
        int maxKeys = webContext.get("max-keys").asInt(1000);
        String marker = webContext.get("marker").asString();
        String prefix = webContext.get("prefix").asString();

        Response response = webContext.respondWith();
        response.setHeader(S3Dispatcher.HTTP_HEADER_NAME_CONTENT_TYPE, S3Dispatcher.CONTENT_TYPE_XML);

        bucket.outputObjectsV1(response.xml(), maxKeys, marker, prefix);
    }

    private void listObjectsV2(WebContext webContext, Bucket bucket) {
        int maxKeys = webContext.get("max-keys").asInt(1000);
        String marker = webContext.get("start-after").asString();
        String prefix = webContext.get("prefix").asString();
        String continuationToken = webContext.get("continuation-token").asString();

        Response response = webContext.respondWith();
        response.setHeader(S3Dispatcher.HTTP_HEADER_NAME_CONTENT_TYPE, S3Dispatcher.CONTENT_TYPE_XML);

        bucket.outputObjectsV2(response.xml(), maxKeys, marker, prefix, continuationToken);
    }
}
