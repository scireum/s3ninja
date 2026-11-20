/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
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
     * The <tt>Authorization</tt> header of the request is of no form which is understood.
     */
    AuthorizationHeaderMalformed(HttpResponseStatus.BAD_REQUEST),

    /**
     * A parameter of a presigned URL which is part of its signature is not valid.
     */
    AuthorizationQueryParametersError(HttpResponseStatus.BAD_REQUEST),

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
     * The upload exceeds the maximum size which is permitted for it.
     */
    EntityTooLarge(HttpResponseStatus.BAD_REQUEST),

    /**
     * A part of a multipart upload other than the last one is smaller than the minimum part size.
     */
    EntityTooSmall(HttpResponseStatus.BAD_REQUEST),

    /**
     * During upload, less than the number of bytes specified have been transmitted.
     */
    IncompleteBody(HttpResponseStatus.BAD_REQUEST),

    /**
     * An internal error has occurred.
     */
    InternalError(HttpResponseStatus.INTERNAL_SERVER_ERROR),

    /**
     * The access key the request was signed for is not known.
     */
    InvalidAccessKeyId(HttpResponseStatus.FORBIDDEN),

    /**
     * An argument of the request is not valid.
     */
    InvalidArgument(HttpResponseStatus.BAD_REQUEST),

    /**
     * The specified bucket name does not adhere to the naming rules.
     */
    InvalidBucketName(HttpResponseStatus.BAD_REQUEST),

    /**
     * The specified checksum value is invalid.
     */
    InvalidDigest(HttpResponseStatus.BAD_REQUEST),

    /**
     * One or more of the parts named when completing a multipart upload could not be found, or an entity tag did not
     * match.
     */
    InvalidPart(HttpResponseStatus.BAD_REQUEST),

    /**
     * The parts named when completing a multipart upload were not listed in ascending order.
     */
    InvalidPartOrder(HttpResponseStatus.BAD_REQUEST),

    /**
     * The requested range of an object lies outside of it.
     */
    InvalidRange(HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE),

    /**
     * The current request is not valid.
     */
    InvalidRequest(HttpResponseStatus.BAD_REQUEST),

    /**
     * The specified object key is longer than permitted.
     */
    KeyTooLongError(HttpResponseStatus.BAD_REQUEST),

    /**
     * The XML sent along with the request was not well-formed or did not match the expected structure.
     */
    MalformedXML(HttpResponseStatus.BAD_REQUEST),

    /**
     * The HTTP method of the request is not supported for the addressed resource.
     */
    MethodNotAllowed(HttpResponseStatus.METHOD_NOT_ALLOWED),

    /**
     * The specified bucket does not exist.
     */
    NoSuchBucket(HttpResponseStatus.NOT_FOUND),

    /**
     * The specified bucket does not have a CORS configuration.
     */
    NoSuchCORSConfiguration(HttpResponseStatus.NOT_FOUND),

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
     * The request asks for a part of the S3 API which is not provided.
     */
    NotImplemented(HttpResponseStatus.NOT_IMPLEMENTED),

    /**
     * A condition the request attached to reading, writing or copying an object does not hold.
     */
    PreconditionFailed(HttpResponseStatus.PRECONDITION_FAILED),

    /**
     * The time at which the request was signed is too far from the time of the server.
     */
    RequestTimeTooSkewed(HttpResponseStatus.FORBIDDEN),

    /**
     * The provided request signature does not match the one calculated by the server.
     */
    SignatureDoesNotMatch(HttpResponseStatus.FORBIDDEN),

    /**
     * The SHA-256 hash stated for the content of the request does not match the content received.
     */
    XAmzContentSHA256Mismatch(HttpResponseStatus.BAD_REQUEST);

    private final HttpResponseStatus httpStatusCode;

    S3ErrorCode(HttpResponseStatus httpStatusCode) {
        this.httpStatusCode = httpStatusCode;
    }

    public HttpResponseStatus getHttpStatusCode() {
        return httpStatusCode;
    }
}
