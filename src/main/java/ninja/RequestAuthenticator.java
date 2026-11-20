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
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.web.http.WebContext;

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
            token = webContext.getHeader("X-Amz-Security-Token");
        }
        if (Strings.isEmpty(token)) {
            // Check as form field (for POST requests)
            token = webContext.get("X-Amz-Security-Token").asString();
        }
        if (Strings.isEmpty(token)) {
            // Also accept the lowercase form field commonly used by AWS clients
            token = webContext.get("x-amz-security-token").asString(webContext.get("X-Amz-Security-Token").asString());
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

        // Allow policy-signed POST uploads without Authorization header; signature is validated later.
        if (isPolicyBasedPost(webContext)) {
            return true;
        }

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

    private boolean isPolicyBasedPost(WebContext webContext) {
        if (!HttpMethod.POST.equals(webContext.getRequest().method())) {
            return false;
        }
        return webContext.hasParameter("policy") || webContext.hasParameter("Policy") || webContext.hasParameter(
                "x-amz-signature") || webContext.hasParameter("Signature");
    }
}
