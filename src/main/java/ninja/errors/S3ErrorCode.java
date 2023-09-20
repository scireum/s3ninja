/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja.errors;

import io.netty.handler.codec.http.HttpResponseStatus;
import sirius.kernel.commons.Explain;

/**
 * Lists some <em>S3</em> <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html">error codes</a>
 * along with their respective {@linkplain HttpResponseStatus HTTP response codes}.
 */
@SuppressWarnings("java:S115")
@Explain("We use the proper names as defined in the AWS API")
public enum S3ErrorCode {
    /**
     * Access denied.
     */
    AccessDenied(HttpResponseStatus.FORBIDDEN),

    /**
     * During upload, the specified checksum value did not match the calculated one.
     */
    BadDigest(HttpResponseStatus.BAD_REQUEST),

    /**
     * The specified bucket name is already in use by somebody else.
     */
    BucketAlreadyExists(HttpResponseStatus.CONFLICT),

    /**
     * The specified bucket name is already in use by yourself.
     */
    BucketAlreadyOwnedByYou(HttpResponseStatus.CONFLICT),

    /**
     * The specified bucket can not be deleted as it is not empty.
     */
    BucketNotEmpty(HttpResponseStatus.CONFLICT),

    /**
     * During upload, less than the number of bytes specified have been transmitted.
     */
    IncompleteBody(HttpResponseStatus.BAD_REQUEST),

    /**
     * An internal error has occurred.
     */
    InternalError(HttpResponseStatus.INTERNAL_SERVER_ERROR),

    /**
     * The specified checksum value is invalid.
     */
    InvalidDigest(HttpResponseStatus.BAD_REQUEST),

    /**
     * The current request is not valid.
     */
    InvalidRequest(HttpResponseStatus.BAD_REQUEST),

    /**
     * The specified bucket does not exist.
     */
    NoSuchBucket(HttpResponseStatus.NOT_FOUND),

    /**
     * The specified bucket does not have a policy.
     */
    NoSuchBucketPolicy(HttpResponseStatus.NOT_FOUND),

    /**
     * The specified key does not exist.
     */
    NoSuchKey(HttpResponseStatus.NOT_FOUND),

    /**
     * The specified lifecycle configuration does not exist.
     */
    NoSuchLifecycleConfiguration(HttpResponseStatus.NOT_FOUND),

    /**
     * The specified multipart upload does not exist.
     */
    NoSuchUpload(HttpResponseStatus.NOT_FOUND),

    /**
     * The provided request signature does not match the one calculated by the server.
     */
    SignatureDoesNotMatch(HttpResponseStatus.FORBIDDEN);

    private final HttpResponseStatus httpStatusCode;

    S3ErrorCode(HttpResponseStatus httpStatusCode) {
        this.httpStatusCode = httpStatusCode;
    }

    public HttpResponseStatus getHttpStatusCode() {
        return httpStatusCode;
    }
}
