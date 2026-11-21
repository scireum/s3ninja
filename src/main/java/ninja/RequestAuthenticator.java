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
import ninja.errors.S3ErrorCode;
import ninja.errors.S3ErrorSynthesizer;
import sirius.kernel.async.CallContext;
import sirius.kernel.cache.Cache;
import sirius.kernel.cache.CacheManager;
import sirius.kernel.commons.Hasher;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.web.http.WebContext;

import java.util.Optional;
import java.util.regex.Matcher;

/**
 * Decides whether a request may access a bucket or an object.
 * <p>
 * A request is let through if it carries a valid session token as handed out to the UI, if it is a browser-based
 * upload of which the policy is verified later on, if its signature matches, or if it carries no signature and the
 * bucket is public.
 */
@Register(classes = RequestAuthenticator.class)
public class RequestAuthenticator {

    /**
     * Names the header, form field or parameter which carries a session token.
     */
    private static final String SECURITY_TOKEN = "X-Amz-Security-Token";

    /**
     * Session tokens for UI access.
     * LocalCache handles automatic expiration after 24 hours.
     */
    private static final Cache<String, Long> sessionTokens = CacheManager.createLocalCache("session-tokens");

    @Part
    private APILog log;

    @Part
    private AwsHashCalculator hashCalculator;

    @Part
    private S3ErrorSynthesizer errorSynthesizer;

    @Part
    private CredentialVerifier credentialVerifier;

    @Part
    private PostPolicyVerifier postPolicyVerifier;

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
        Matcher matcher = AwsHashCalculator.AWS_AUTH_PATTERN.matcher(authentication);
        if (matcher.matches()) {
            return matcher.group(2);
        }

        matcher = Aws4HashCalculator.AWS_AUTH4_PATTERN.matcher(authentication);
        if (matcher.matches()) {
            return matcher.group(7);
        }

        return null;
    }

    /**
     * Generates a new session token for UI access.
     * Token is valid for 24 hours.
     *
     * @return the generated session token
     */
    public String generateSessionToken() {
        String token = Hasher.md5().hash(System.currentTimeMillis() + "-" + Math.random()).toHexString();
        long expirationTime = System.currentTimeMillis() + (24 * 60 * 60 * 1000); // 24 hours
        sessionTokens.put(token, expirationTime);

        return token;
    }

    /**
     * Validates a session token.
     *
     * @param token the token to validate
     * @return true if the token is valid and not expired, false otherwise
     */
    public boolean validateSessionToken(String token) {
        if (Strings.isEmpty(token)) {
            return false;
        }
        Long expirationTime = sessionTokens.get(token);
        if (expirationTime == null) {
            S3Dispatcher.LOG.FINE("Session token validation failed: token not found or expired");
            return false;
        }
        if (System.currentTimeMillis() > expirationTime) {
            S3Dispatcher.LOG.FINE("Session token validation failed: token expired");
            return false;
        }
        S3Dispatcher.LOG.FINE("Session token validated successfully");
        return true;
    }

    /**
     * Extracts the session token from the request.
     * Checks X-Session-Token header, X-Amz-Security-Token header/field, and sessionToken parameter.
     *
     * @param webContext the context describing the current request
     * @return the session token if present, null otherwise
     */
    private String extractSessionToken(WebContext webContext) {
        // Check X-Session-Token header (custom header for direct use)
        String token = webContext.getHeader("X-Session-Token");
        if (Strings.isEmpty(token)) {
            // Check X-Amz-Security-Token (AWS standard for temporary credentials)
            token = webContext.getHeader(SECURITY_TOKEN);
        }
        if (Strings.isEmpty(token)) {
            // Check as form field (for POST requests)
            token = webContext.get(SECURITY_TOKEN).asString();
        }
        if (Strings.isEmpty(token)) {
            // Also accept the lowercase form field commonly used by AWS clients
            token = webContext.get("x-amz-security-token").asString(webContext.get(SECURITY_TOKEN).asString());
        }
        if (Strings.isEmpty(token)) {
            // Check as query parameter
            token = webContext.get("sessionToken").asString();
        }
        return token;
    }

    boolean objectCheckAuth(WebContext webContext, Bucket bucket, String key) {
        // Check for session token first
        String sessionToken = extractSessionToken(webContext);
        if (Strings.isFilled(sessionToken) && validateSessionToken(sessionToken)) {
            return true;
        }

        // A browser-based upload carries no signature of its own. Its policy is verified before anything is stored.
        if (isPolicyBasedPost(webContext)) {
            return true;
        }

        if (hasMalformedAuthorization(webContext, bucket.getName(), key)) {
            return false;
        }

        String hash = getAuthHash(webContext);
        if (Strings.isFilled(hash) && !verifySignature(webContext, hash, bucket.getName(), key)) {
            return false;
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
     * Verifies the signature of a request which addresses no bucket, i.e. which lists all buckets.
     * <p>
     * Such a request is served without a signature. One which is signed has to be signed correctly though, as a
     * client would otherwise never learn that its credentials are wrong.
     *
     * @param webContext the request to verify
     * @return <tt>true</tt> if the request may be served, <tt>false</tt> if it has been answered with an error
     */
    boolean checkSignatureIfPresent(WebContext webContext) {
        String sessionToken = extractSessionToken(webContext);
        if (Strings.isFilled(sessionToken) && validateSessionToken(sessionToken)) {
            return true;
        }

        if (hasMalformedAuthorization(webContext, null, null)) {
            return false;
        }

        String hash = getAuthHash(webContext);
        return Strings.isEmpty(hash) || verifySignature(webContext, hash, null, null);
    }

    /**
     * Refuses a request of which the <tt>Authorization</tt> header is of no form which is understood.
     * <p>
     * Such a request carries no signature which could be verified, but must not be served like one which was never
     * meant to be signed.
     *
     * @return <tt>true</tt> if the request has been answered with an error, <tt>false</tt> if it may be processed
     */
    private boolean hasMalformedAuthorization(WebContext webContext, String bucketName, String key) {
        if (webContext.getHeaderValue(HttpHeaderNames.AUTHORIZATION).isEmptyString() || Strings.isFilled(getAuthHash(
                webContext))) {
            return false;
        }

        errorSynthesizer.synthesiseError(webContext,
                                         bucketName,
                                         key,
                                         S3ErrorCode.AuthorizationHeaderMalformed,
                                         "The authorization header that you provided is not valid.");
        log.log(webContext.getRequest().method().name(),
                webContext.getRequestedURI(),
                APILog.Result.REJECTED,
                CallContext.getCurrent().getWatch());
        return true;
    }

    /**
     * Verifies the credentials and the signature of a signed request, and answers it with an error if they are wrong.
     *
     * @return <tt>true</tt> if the request is signed correctly, <tt>false</tt> if it has been answered with an error
     */
    private boolean verifySignature(WebContext webContext, String hash, String bucketName, String key) {
        Optional<Tuple<S3ErrorCode, String>> credentialError = credentialVerifier.verify(webContext);
        if (credentialError.isPresent()) {
            errorSynthesizer.synthesiseError(webContext,
                                             bucketName,
                                             key,
                                             credentialError.get().getFirst(),
                                             credentialError.get().getSecond());
            log.log(webContext.getRequest().method().name(),
                    webContext.getRequestedURI(),
                    APILog.Result.REJECTED,
                    CallContext.getCurrent().getWatch());
            return false;
        }

        String expectedHash = hashCalculator.computeHash(webContext, "");
        String alternativeHash = hashCalculator.computeHash(webContext, "/s3");
        if (!expectedHash.equals(hash) && !alternativeHash.equals(hash)) {
            errorSynthesizer.synthesiseError(webContext,
                                             bucketName,
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

        return true;
    }

    /**
     * Determines if the request is a browser-based upload, which is not signed itself, but carries a signed policy.
     * <p>
     * Such a request is let through here, as {@link PostPolicyVerifier} verifies its policy and signature before
     * anything is stored. The other POST requests of the API, which initiate or complete a multipart upload or delete
     * several objects, are never exempt, as nothing would verify a policy sent along with them.
     */
    private boolean isPolicyBasedPost(WebContext webContext) {
        if (!HttpMethod.POST.equals(webContext.getRequest().method())) {
            return false;
        }
        if (webContext.hasParameter("uploads") || webContext.hasParameter("uploadId") || webContext.hasParameter(
                "delete")) {
            return false;
        }
        return postPolicyVerifier.hasPolicy(webContext);
    }
}
