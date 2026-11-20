/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.io.BaseEncoding;
import ninja.errors.S3ErrorCode;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.web.http.WebContext;

import javax.annotation.Nullable;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.regex.Matcher;

/**
 * Verifies the policy of a browser-based upload, which is a POST request carrying a form.
 * <p>
 * Such a request is not signed itself. It carries a policy stating what may be uploaded until when, and a signature
 * of that policy. The request is to be served if the signature has been made with the secret key, the policy has not
 * expired, and the request meets all of its conditions.
 * <p>
 * As for S3, what is signed is the policy exactly as it is sent, i.e. in its Base64 encoding. This holds for
 * signature version 4, sent as <tt>x-amz-signature</tt>, and for version 2, sent as <tt>signature</tt>.
 */
@Register(classes = PostPolicyVerifier.class)
public class PostPolicyVerifier {

    /**
     * Names the algorithm signature version 4 derives its keys and signatures with.
     */
    private static final String HMAC_SHA256 = "HmacSHA256";

    private static final String ALGORITHM_VERSION_4 = "AWS4-HMAC-SHA256";
    /**
     * Explains that the size of an upload lies outside of the <tt>content-length-range</tt> of its policy.
     */
    public static final String ERROR_SIZE_NOT_PERMITTED =
            "Your proposed upload does not meet the content-length-range condition of its policy.";

    private static final String ERROR_SIGNATURE_DOES_NOT_MATCH =
            "The request signature we calculated does not match the signature you provided";

    /**
     * Describes the outcome of verifying a browser-based upload.
     *
     * @param policy    the policy of the upload, or <tt>null</tt> if the upload is to be refused
     * @param errorCode the error to refuse the upload with, or <tt>null</tt> if it is to be served
     * @param message   the message which explains the error, or <tt>null</tt> if the upload is to be served
     */
    public record Result(@Nullable S3Policy policy, @Nullable S3ErrorCode errorCode, @Nullable String message) {

        /**
         * Determines if the upload is to be served.
         *
         * @return <tt>true</tt> if policy and signature are in order, <tt>false</tt> if the upload is to be refused
         */
        public boolean isValid() {
            return errorCode == null;
        }

        private static Result refused(S3ErrorCode errorCode, String message) {
            return new Result(null, errorCode, message);
        }
    }

    @Part
    private Storage storage;

    /**
     * Determines if the given request carries a policy, and is therefore a browser-based upload.
     *
     * @param webContext the request to check
     * @return <tt>true</tt> if a policy is sent along as form field or parameter, <tt>false</tt> otherwise
     */
    public boolean hasPolicy(WebContext webContext) {
        return Strings.isFilled(readField(webContext, "policy", "Policy"));
    }

    /**
     * Verifies the policy and the signature sent along with the given request.
     *
     * @param webContext the request to verify
     * @param bucket     the bucket to upload into
     * @param key        the key of the object to upload
     * @return the policy if the upload is to be served, or the error to refuse it with
     */
    public Result verify(WebContext webContext, Bucket bucket, String key) {
        String policy = readField(webContext, "policy", "Policy");
        String signatureVersion4 = readField(webContext, "x-amz-signature", "X-Amz-Signature");
        String algorithm = readField(webContext, "x-amz-algorithm", "X-Amz-Algorithm");
        String signatureVersion2 = readField(webContext, "signature", "Signature");

        boolean signedWithVersion4 = Strings.isFilled(signatureVersion4) && Strings.isFilled(algorithm);
        if (Strings.isEmpty(policy) || (!signedWithVersion4 && Strings.isEmpty(signatureVersion2))) {
            return Result.refused(S3ErrorCode.AccessDenied, "Missing policy or signature for policy-based POST");
        }

        if (!signedWithVersion4 && !Strings.areEqual(webContext.getParameter("AWSAccessKeyId"),
                                                     storage.getAwsAccessKey())) {
            return Result.refused(S3ErrorCode.InvalidAccessKeyId,
                                  "The AWS Access Key Id you provided does not exist in our records.");
        }

        boolean signatureMatches = signedWithVersion4 ?
                                   matchesVersion4(webContext, policy, algorithm, signatureVersion4) :
                                   matchesVersion2(policy, signatureVersion2);
        if (!signatureMatches) {
            return Result.refused(S3ErrorCode.SignatureDoesNotMatch, ERROR_SIGNATURE_DOES_NOT_MATCH);
        }

        try {
            S3Policy parsedPolicy = new S3Policy(policy);
            if (!parsedPolicy.isRequestValid(webContext, bucket, key)) {
                return Result.refused(S3ErrorCode.AccessDenied, "Policy conditions not met");
            }
            return new Result(parsedPolicy, null, null);
        } catch (IllegalArgumentException exception) {
            return Result.refused(S3ErrorCode.InvalidRequest, "Policy parsing error: " + exception.getMessage());
        }
    }

    /**
     * Checks the size of an upload against the <tt>content-length-range</tt> condition of its policy.
     *
     * @param policy the policy of the upload
     * @param size   the size of the uploaded content in bytes
     * @return the error to refuse the upload with, or <tt>null</tt> if the policy permits the size
     */
    @Nullable
    public S3ErrorCode verifySize(S3Policy policy, long size) {
        if (policy.isPermittedSize(size)) {
            return null;
        }
        return policy.isBelowMinimumSize(size) ? S3ErrorCode.EntityTooSmall : S3ErrorCode.EntityTooLarge;
    }

    /**
     * Reads a form field which clients send in either of two spellings.
     */
    private String readField(WebContext webContext, String name, String alternativeName) {
        String value = webContext.getParameter(name);
        return Strings.isFilled(value) ? value : webContext.getParameter(alternativeName);
    }

    private boolean matchesVersion4(WebContext webContext, String policy, String algorithm, String signature) {
        String credential = readField(webContext, "x-amz-credential", "X-Amz-Credential");
        if (!ALGORITHM_VERSION_4.equals(algorithm) || Strings.isEmpty(credential)) {
            return false;
        }

        Matcher credentialMatcher = Aws4HashCalculator.X_AMZ_CREDENTIAL_PATTERN.matcher(credential);
        if (!credentialMatcher.matches()
            || !Strings.areEqual(credentialMatcher.group(1), storage.getAwsAccessKey())
            || !"aws4_request".equals(credentialMatcher.group(5))) {
            return false;
        }

        try {
            byte[] secretKey = ("AWS4" + storage.getAwsSecretKey()).getBytes(StandardCharsets.UTF_8);
            byte[] dateKey = hmac(HMAC_SHA256, secretKey, credentialMatcher.group(2));
            byte[] regionKey = hmac(HMAC_SHA256, dateKey, credentialMatcher.group(3));
            byte[] serviceKey = hmac(HMAC_SHA256, regionKey, credentialMatcher.group(4));
            byte[] signingKey = hmac(HMAC_SHA256, serviceKey, "aws4_request");

            String expectedSignature = BaseEncoding.base16().lowerCase().encode(hmac(HMAC_SHA256, signingKey, policy));
            return expectedSignature.equals(signature);
        } catch (GeneralSecurityException exception) {
            Exceptions.handle(Storage.LOG, exception);
            return false;
        }
    }

    private boolean matchesVersion2(String policy, String signature) {
        try {
            byte[] secretKey = storage.getAwsSecretKey().getBytes(StandardCharsets.UTF_8);
            return BaseEncoding.base64().encode(hmac("HmacSHA1", secretKey, policy)).equals(signature);
        } catch (GeneralSecurityException exception) {
            Exceptions.handle(Storage.LOG, exception);
            return false;
        }
    }

    private byte[] hmac(String algorithm, byte[] key, String value) throws GeneralSecurityException {
        Mac mac = Mac.getInstance(algorithm);
        mac.init(new SecretKeySpec(key, algorithm));
        return mac.doFinal(value.getBytes(StandardCharsets.UTF_8));
    }
}
