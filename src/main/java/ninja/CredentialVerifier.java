/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import io.netty.handler.codec.http.HttpHeaderNames;
import ninja.errors.S3ErrorCode;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.web.http.WebContext;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.regex.Matcher;

/**
 * Verifies what a signed request states next to its signature: the access key it was signed for, and the time at
 * which, or until which, it is valid.
 * <p>
 * The signature itself is verified by the {@link AwsHashCalculator}. A request which carries no signature at all is
 * none of this class' business.
 */
@Register(classes = CredentialVerifier.class)
public class CredentialVerifier {

    /**
     * Contains the difference S3 tolerates between the time a request was signed at and its own clock.
     */
    private static final Duration MAX_CLOCK_SKEW = Duration.ofMinutes(15);

    private static final DateTimeFormatter AMAZON_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'");

    private static final String ERROR_INVALID_ACCESS_KEY =
            "The AWS Access Key Id you provided does not exist in our records.";
    private static final String ERROR_REQUEST_EXPIRED = "Request has expired";
    private static final String ERROR_REQUEST_TIME_TOO_SKEWED =
            "The difference between the request time and the current time is too large.";

    /**
     * Contains the number of seconds S3 permits a presigned URL to be valid for at most, which is a week.
     */
    private static final long MAX_PRESIGNED_SECONDS = 604800;

    private static final String ERROR_EXPIRES_TOO_LARGE =
            "X-Amz-Expires must be less than a week (in seconds); that is, the given X-Amz-Expires must be less than 604800 seconds";

    @Part
    private Storage storage;

    /**
     * Verifies the access key and the time of the given request.
     *
     * @param webContext the request to verify
     * @return the error to answer the request with, or an empty optional if nothing is wrong
     */
    public Optional<Tuple<S3ErrorCode, String>> verify(WebContext webContext) {
        String accessKey = determineAccessKey(webContext);
        if (Strings.isFilled(accessKey) && !Strings.areEqual(accessKey, storage.getAwsAccessKey())) {
            return Optional.of(Tuple.create(S3ErrorCode.InvalidAccessKeyId, ERROR_INVALID_ACCESS_KEY));
        }

        if (webContext.get("X-Amz-Signature").isFilled()
            && webContext.get("X-Amz-Expires").asLong(0) > MAX_PRESIGNED_SECONDS) {
            return Optional.of(Tuple.create(S3ErrorCode.AuthorizationQueryParametersError, ERROR_EXPIRES_TOO_LARGE));
        }

        Instant now = Instant.now();
        if (isExpiredPresignedUrl(webContext, now)) {
            return Optional.of(Tuple.create(S3ErrorCode.AccessDenied, ERROR_REQUEST_EXPIRED));
        }
        if (isSignedAtSkewedTime(webContext, now)) {
            return Optional.of(Tuple.create(S3ErrorCode.RequestTimeTooSkewed, ERROR_REQUEST_TIME_TOO_SKEWED));
        }

        return Optional.empty();
    }

    /**
     * Extracts the access key from the <tt>Authorization</tt> header or, for presigned URLs, from the query string.
     *
     * @return the access key, or <tt>null</tt> if the request names none
     */
    private String determineAccessKey(WebContext webContext) {
        String authorization = webContext.getHeaderValue(HttpHeaderNames.AUTHORIZATION).asString("");

        Matcher matcher = Aws4HashCalculator.AWS_AUTH4_PATTERN.matcher(authorization);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        matcher = AwsHashCalculator.AWS_AUTH_PATTERN.matcher(authorization);
        if (matcher.matches()) {
            return matcher.group(1);
        }

        String credential = webContext.get("X-Amz-Credential").asString();
        if (Strings.isFilled(credential)) {
            return Strings.split(credential, "/").getFirst();
        }
        return webContext.get("AWSAccessKeyId").getString();
    }

    /**
     * Determines if the request is a presigned URL which is no longer valid.
     * <p>
     * A URL signed with signature version 4 states when it was signed and for how many seconds it is valid, one signed
     * with version 2 states the second at which it expires.
     */
    private boolean isExpiredPresignedUrl(WebContext webContext, Instant now) {
        if (webContext.get("X-Amz-Signature").isFilled()) {
            Optional<Instant> signedAt = parseAmazonDate(webContext.get("X-Amz-Date").asString());
            long validSeconds = webContext.get("X-Amz-Expires").asLong(-1);
            return signedAt.isPresent() && validSeconds >= 0 && now.isAfter(signedAt.get().plusSeconds(validSeconds));
        }

        if (webContext.get("Signature").isFilled()) {
            long expires = webContext.get("Expires").asLong(-1);
            return expires >= 0 && now.isAfter(Instant.ofEpochSecond(expires));
        }

        return false;
    }

    /**
     * Determines if the request has been signed via its <tt>Authorization</tt> header at a time which is too far
     * from now, which hints at a wrong clock or at a request being replayed.
     */
    private boolean isSignedAtSkewedTime(WebContext webContext, Instant now) {
        if (webContext.getHeaderValue(HttpHeaderNames.AUTHORIZATION).isEmptyString()) {
            return false;
        }

        Optional<Instant> signedAt = parseAmazonDate(webContext.getHeader("x-amz-date"));
        return signedAt.isPresent() && Duration.between(signedAt.get(), now).abs().compareTo(MAX_CLOCK_SKEW) > 0;
    }

    private Optional<Instant> parseAmazonDate(String date) {
        if (Strings.isEmpty(date)) {
            return Optional.empty();
        }

        try {
            return Optional.of(LocalDateTime.parse(date, AMAZON_DATE_FORMAT).toInstant(ZoneOffset.UTC));
        } catch (DateTimeParseException exception) {
            return Optional.empty();
        }
    }
}
