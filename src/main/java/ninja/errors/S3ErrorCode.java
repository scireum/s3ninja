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
    AccessDenied(HttpResponseStatus.FORBIDDEN), BadDigest(HttpResponseStatus.BAD_REQUEST),
    IncompleteBody(HttpResponseStatus.BAD_REQUEST), InternalError(HttpResponseStatus.INTERNAL_SERVER_ERROR),
    InvalidDigest(HttpResponseStatus.BAD_REQUEST), InvalidRequest(HttpResponseStatus.BAD_REQUEST),
    NoSuchBucket(HttpResponseStatus.NOT_FOUND), NoSuchBucketPolicy(HttpResponseStatus.NOT_FOUND),
    NoSuchKey(HttpResponseStatus.NOT_FOUND), NoSuchLifecycleConfiguration(HttpResponseStatus.NOT_FOUND),
    NoSuchUpload(HttpResponseStatus.NOT_FOUND), SignatureDoesNotMatch(HttpResponseStatus.FORBIDDEN);

    private final HttpResponseStatus httpStatusCode;

    S3ErrorCode(HttpResponseStatus httpStatusCode) {
        this.httpStatusCode = httpStatusCode;
    }

    public HttpResponseStatus getHttpStatusCode() {
        return httpStatusCode;
    }
}
