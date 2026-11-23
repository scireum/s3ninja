/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.io.BaseEncoding;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.InterfaceHttpPostRequestDecoder;
import ninja.errors.S3ErrorCode;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.web.http.WebContext;

import javax.annotation.Nullable;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
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

    /**
     * Lists, in lower case, the form fields which no condition of the policy has to cover.
     */
    private static final Set<String> FIELDS_EXEMPT_FROM_POLICY =
            Set.of("policy", "x-amz-signature", "signature", "awsaccesskeyid", "file");

    /**
     * Starts the names of form fields which S3 ignores, and which therefore need no condition.
     */
    private static final String IGNORED_FIELD_PREFIX = "x-ignore-";

    private static final String FIELD_FILE = "file";

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

        if (!signedWithVersion4 && !Strings.areEqual(readFormField(webContext, "AWSAccessKeyId").getString(),
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
            Optional<String> uncoveredField = findUncoveredField(webContext, parsedPolicy);
            if (uncoveredField.isPresent()) {
                return Result.refused(S3ErrorCode.AccessDenied,
                                      "Invalid according to Policy: Extra input fields: " + uncoveredField.get());
            }
            return new Result(parsedPolicy, null, null);
        } catch (IllegalArgumentException exception) {
            return Result.refused(S3ErrorCode.InvalidRequest, "Policy parsing error: " + exception.getMessage());
        }
    }

    /**
     * Finds a field of the form which no condition of the policy covers.
     * <p>
     * As for S3, every field has to be covered, except the policy, its signature, the access key of a form signed with
     * signature version 2, the file, and fields starting with <tt>x-ignore-</tt>. Otherwise anyone holding a signed
     * form could add fields, such as metadata, which its policy never permitted.
     *
     * @param webContext the request carrying the form
     * @param policy     the policy of the form
     * @return the name of the first field which is not covered, or an empty optional if all are
     */
    private Optional<String> findUncoveredField(WebContext webContext, S3Policy policy) {
        Set<String> fieldsAfterFile = determineFieldsAfterFile(webContext);
        return webContext.getParameterNames()
                         .stream()
                         .filter(name -> !isExemptFromPolicy(name))
                         .filter(name -> !fieldsAfterFile.contains(name))
                         .filter(name -> !policy.coversField(name))
                         .findFirst();
    }

    /**
     * Reads a field of a form, as S3 does: names are compared without regard to case, and only the first field of
     * that name before the file counts, as every field after the file is ignored.
     * <p>
     * Parameters of the query are read if the form has no such field, as a form may also be sent as the query of an
     * upload.
     *
     * @param webContext the request carrying the form
     * @param name       the name of the field
     * @return the value of the field, which is empty if neither the form before its file nor the query has one
     */
    public static Value readFormField(WebContext webContext, String name) {
        InterfaceHttpPostRequestDecoder decoder = webContext.getPostDecoder();
        if (decoder == null) {
            return webContext.get(name);
        }
        try {
            for (InterfaceHttpData data : decoder.getBodyHttpDatas()) {
                if (FIELD_FILE.equalsIgnoreCase(data.getName())) {
                    break;
                }
                if (data instanceof Attribute attribute && name.equalsIgnoreCase(attribute.getName())) {
                    return Value.of(attribute.getValue());
                }
            }
        } catch (Exception exception) {
            // A form which cannot be decoded is refused when its fields are read.
            Exceptions.ignore(exception);
        }
        // The body joins all fields of a name, including those after the file, so it may only be asked for the query.
        return determineFieldsAfterFile(webContext).contains(name) ? Value.EMPTY : webContext.get(name);
    }

    /**
     * Determines the fields of a form which only come after its file.
     * <p>
     * The file has to be the last field of a form, and S3 ignores every field after it. These fields therefore need
     * no condition, and are not stored along with the object.
     *
     * @param webContext the request carrying the form
     * @return the names of the fields which only come after the file, compared without regard to case, or an empty
     * set if the request is no form
     */
    public static Set<String> determineFieldsAfterFile(WebContext webContext) {
        InterfaceHttpPostRequestDecoder decoder = webContext.getPostDecoder();
        if (decoder == null) {
            return Set.of();
        }

        Set<String> fieldsBeforeFile = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Set<String> fieldsAfterFile = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        boolean fileSeen = false;
        try {
            for (InterfaceHttpData data : decoder.getBodyHttpDatas()) {
                if (fileSeen) {
                    fieldsAfterFile.add(data.getName());
                } else if (FIELD_FILE.equalsIgnoreCase(data.getName())) {
                    fileSeen = true;
                } else {
                    fieldsBeforeFile.add(data.getName());
                }
            }
        } catch (Exception exception) {
            // A form which cannot be decoded is refused when its fields are read.
            Exceptions.ignore(exception);
            return Set.of();
        }
        fieldsAfterFile.removeIf(fieldsBeforeFile::contains);
        return fieldsAfterFile;
    }

    private boolean isExemptFromPolicy(String fieldName) {
        String name = fieldName.toLowerCase(Locale.ROOT);
        return FIELDS_EXEMPT_FROM_POLICY.contains(name) || name.startsWith(IGNORED_FIELD_PREFIX);
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
        String value = readFormField(webContext, name).getString();
        return Strings.isFilled(value) ? value : readFormField(webContext, alternativeName).getString();
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
            return Base64.getEncoder().encodeToString(hmac("HmacSHA1", secretKey, policy)).equals(signature);
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
