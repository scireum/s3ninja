/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import ninja.errors.S3ErrorCode;
import ninja.errors.S3ErrorSynthesizer;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.web.http.InputStreamHandler;
import sirius.web.http.WebContext;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.regex.Matcher;

/**
 * Handles browser-based uploads, which are POST requests carrying a signed policy that states what may be
 * uploaded.
 */
@Register(classes = PostObjectHandler.class)
public class PostObjectHandler {

    @Part
    private Storage storage;

    @Part
    private S3ErrorSynthesizer errorSynthesizer;

    @Part
    private ObjectHandler objectHandler;

    /**
     * Handles a standard POST request for object creation
     * Supports both form-based uploads and AWS policy-based uploads
     */
    void handlePostObject(WebContext webContext, Bucket bucket, String key, InputStreamHandler data)
            throws IOException {
        // Check for AWS4 signature parameters (modern AWS S3 POST)
        String policy = webContext.getParameter("policy");
        String xAmzSignature = webContext.getParameter("x-amz-signature");
        String xAmzAlgorithm = webContext.getParameter("x-amz-algorithm");

        // Also accept the common "X-Amz-*" form field casing
        if (Strings.isEmpty(policy)) {
            policy = webContext.getParameter("Policy");
        }
        if (Strings.isEmpty(xAmzSignature)) {
            xAmzSignature = webContext.getParameter("X-Amz-Signature");
        }
        if (Strings.isEmpty(xAmzAlgorithm)) {
            xAmzAlgorithm = webContext.getParameter("X-Amz-Algorithm");
        }

        // Check for legacy signature parameters
        String signature = webContext.getParameter("Signature");

        if (!Strings.isFilled(policy)) {
            // S3 only accepts a POST to an object as browser-based upload, which always carries a policy.
            ByteStreams.exhaust(data);
            errorSynthesizer.synthesiseError(webContext,
                                             bucket.getName(),
                                             key,
                                             S3ErrorCode.MethodNotAllowed,
                                             "The specified method is not allowed against this resource.");
            return;
        }

        // If a policy is present, we must have either an AWS4 signature or a legacy signature.
        // This matches how S3 behaves and prevents unauthenticated policy uploads.
        boolean hasAws4 = Strings.isFilled(xAmzSignature) && Strings.isFilled(xAmzAlgorithm);
        boolean hasLegacy = Strings.isFilled(signature);

        if (!hasAws4 && !hasLegacy) {
            errorSynthesizer.synthesiseError(webContext,
                                             bucket.getName(),
                                             key,
                                             S3ErrorCode.AccessDenied,
                                             "Missing signature for policy-based POST");
            return;
        }

        // AWS signature validation for POST uploads
        if (hasAws4) {
            // AWS4-HMAC-SHA256 signature (modern)
            if (!Strings.areEqual(xAmzAlgorithm, "AWS4-HMAC-SHA256") || !validateAWS4PolicySignature(webContext,
                                                                                                     policy,
                                                                                                     xAmzSignature)) {
                errorSynthesizer.synthesiseError(webContext,
                                                 bucket.getName(),
                                                 key,
                                                 S3ErrorCode.SignatureDoesNotMatch,
                                                 "The request signature we calculated does not match the signature you provided");
                return;
            }
        } else {
            // Legacy signature validation
            if (!validatePolicySignature(policy, signature)) {
                errorSynthesizer.synthesiseError(webContext,
                                                 bucket.getName(),
                                                 key,
                                                 S3ErrorCode.SignatureDoesNotMatch,
                                                 "The request signature we calculated does not match the signature you provided");
                return;
            }
        }

        try {
            S3Policy s3Policy = new S3Policy(policy);

            if (!s3Policy.isRequestValid(webContext, bucket, key)) {
                errorSynthesizer.synthesiseError(webContext,
                                                 bucket.getName(),
                                                 key,
                                                 S3ErrorCode.AccessDenied,
                                                 "Policy conditions not met");
                return;
            }

            long maxFileSize = s3Policy.getMaxFileSize();
            if (maxFileSize > 0 && webContext.getContentSize() > maxFileSize) {
                errorSynthesizer.synthesiseError(webContext,
                                                 bucket.getName(),
                                                 key,
                                                 S3ErrorCode.InvalidDigest,
                                                 "File size exceeds maximum allowed by policy");
                return;
            }

            objectHandler.putObject(webContext, bucket, key, data);

            String redirectUrl = s3Policy.getSuccessRedirect();
            if (Strings.isFilled(redirectUrl)) {
                webContext.respondWith().redirectToGet(redirectUrl);
            } else {
                webContext.respondWith().status(s3Policy.getSuccessResponse());
            }
        } catch (IllegalArgumentException e) {
            errorSynthesizer.synthesiseError(webContext,
                                             bucket.getName(),
                                             key,
                                             S3ErrorCode.InvalidRequest,
                                             "Policy parsing error: " + e.getMessage());
        }
    }

    private boolean validateAWS4PolicySignature(WebContext webContext, String policyBase64, String providedSignature) {
        try {
            String credential = webContext.getParameter("x-amz-credential");
            if (Strings.isEmpty(credential)) {
                credential = webContext.getParameter("X-Amz-Credential");
            }
            if (Strings.isEmpty(credential)) {
                return false;
            }

            Matcher credMatcher = Aws4HashCalculator.X_AMZ_CREDENTIAL_PATTERN.matcher(credential);
            if (!credMatcher.matches()) {
                return false;
            }

            String accessKey = credMatcher.group(1);
            String date = credMatcher.group(2);   // yyyymmdd
            String region = credMatcher.group(3);
            String service = credMatcher.group(4);
            String terminal = credMatcher.group(5);

            if (!Strings.areEqual(accessKey, storage.getAwsAccessKey())) {
                return false;
            }

            String algorithm = webContext.getParameter("x-amz-algorithm");
            if (Strings.isEmpty(algorithm)) {
                algorithm = webContext.getParameter("X-Amz-Algorithm");
            }
            if (!Strings.areEqual(algorithm, "AWS4-HMAC-SHA256")) {
                return false;
            }

            if (!Strings.areEqual(terminal, "aws4_request")) {
                return false;
            }

            byte[] kSecret = ("AWS4" + storage.getAwsSecretKey()).getBytes(StandardCharsets.UTF_8);
            byte[] kDate = hmacSHA256(kSecret, date);
            byte[] kRegion = hmacSHA256(kDate, region);
            byte[] kService = hmacSHA256(kRegion, service);
            byte[] kSigning = hmacSHA256(kService, "aws4_request");

            byte[] policyBytes = java.util.Base64.getDecoder().decode(policyBase64);
            byte[] expectedSig = hmacSHA256(kSigning, policyBytes);
            String expectedHex = BaseEncoding.base16().lowerCase().encode(expectedSig);

            // Backward compatibility: some tools mistakenly sign the base64 string.
            // We accept this only if explicitly enabled.
            if (Strings.areEqual(expectedHex, providedSignature)) {
                return true;
            }

            boolean allowBase64PolicySigning = webContext.get("allowBase64PolicySigning").asBoolean(false);
            if (allowBase64PolicySigning) {
                byte[] expectedSigBase64 = hmacSHA256(kSigning, policyBase64.getBytes(StandardCharsets.UTF_8));
                String expectedHexBase64 = BaseEncoding.base16().lowerCase().encode(expectedSigBase64);
                return Strings.areEqual(expectedHexBase64, providedSignature);
            }

            return false;
        } catch (Exception e) {
            return false;
        }
    }

    private byte[] hmacSHA256(byte[] key, String value) throws NoSuchAlgorithmException, InvalidKeyException {
        SecretKeySpec keySpec = new SecretKeySpec(key, "HmacSHA256");
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(keySpec);
        return mac.doFinal(value.getBytes(StandardCharsets.UTF_8));
    }

    private byte[] hmacSHA256(byte[] key, byte[] value) throws NoSuchAlgorithmException, InvalidKeyException {
        SecretKeySpec keySpec = new SecretKeySpec(key, "HmacSHA256");
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(keySpec);
        return mac.doFinal(value);
    }

    /**
     * Validates legacy AWS S3 POST policy signatures using HMAC-SHA1.
     * This implements proper cryptographic validation to prevent security vulnerabilities.
     *
     * @param policy    The Base64-encoded policy document
     * @param signature The signature to validate against
     * @return true if signature is valid, false otherwise
     */
    private boolean validatePolicySignature(String policy, String signature) {
        try {
            new S3Policy(policy);

            String secretKey = storage.getAwsSecretKey();
            if (Strings.isEmpty(secretKey)) {
                return false;
            }

            SecretKeySpec keySpec = new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8), "HmacSHA1");
            Mac mac = Mac.getInstance("HmacSHA1");
            mac.init(keySpec);

            byte[] expectedSig = mac.doFinal(policy.getBytes(StandardCharsets.UTF_8));
            String expectedSignature = BaseEncoding.base64().encode(expectedSig);

            return Strings.areEqual(expectedSignature, signature);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            Exceptions.handle(e);
            return false;
        } catch (Exception e) {
            return false;
        }
    }
}
