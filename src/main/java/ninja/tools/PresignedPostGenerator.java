package ninja.tools;

import com.google.common.io.BaseEncoding;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

/**
 * Generates a presigned POST policy and signature for testing.
 *
 * @deprecated This manual generator has been replaced by {@code PresignedPostGeneratorTest}
 *             which uses Jackson for proper JSON construction and runs as an automated test.
 *             See {@code src/test/kotlin/PresignedPostGeneratorTest.kt}
 */
@Deprecated
public class PresignedPostGenerator {

    public static void main(String[] args) throws Exception {
        String bucket = "test";
        String key = "okok.txt";
        String region = "eu-west-3";
        String accessKey = "AKIAIOSFODNN7EXAMPLE";
        String secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

        // TODO: Paste your session token here if testing with temporary credentials
        String sessionToken = "047b688c0be3e87dcda3e07";

        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
        String date = DateTimeFormatter.ofPattern("yyyyMMdd").format(now);
        String xAmzDate = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").format(now);

        // Expiration: 24 hours from now
        String expiration = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").format(now.plusHours(24));

        String credential = accessKey + "/" + date + "/" + region + "/s3/aws4_request";

        // Build Policy JSON
        StringBuilder policyJson = new StringBuilder();
        policyJson.append("{\n");
        for (String s : Arrays.asList("  \"expiration\": \"" + expiration + "\",\n",
                                      "  \"conditions\": [\n",
                                      "    {\"bucket\": \"" + bucket + "\"},\n",
                                      "    [\"eq\", \"$key\", \"" + key + "\"],\n",
                                      "    {\"x-amz-algorithm\": \"AWS4-HMAC-SHA256\"},\n",
                                      "    {\"x-amz-credential\": \"" + credential + "\"},",
                                      "    {\"x-amz-date\": \"" + xAmzDate + "\"}",
                                      " {\"x-amz-security-token\": \"" + sessionToken + "\"}\n]\n}")) {
            policyJson.append(s);
        }

        String policyBase64 = BaseEncoding.base64().encode(policyJson.toString().getBytes(StandardCharsets.UTF_8));

        // Calculate Signature
        byte[] kSecret = ("AWS4" + secretKey).getBytes(StandardCharsets.UTF_8);
        byte[] kDate = hmacSHA256(kSecret, date);
        byte[] kRegion = hmacSHA256(kDate, region);
        byte[] kService = hmacSHA256(kRegion, "s3");
        byte[] kSigning = hmacSHA256(kService, "aws4_request");

        // Per AWS S3 SigV4 POST, sign the *decoded* policy document bytes (not the base64 string).
        byte[] signatureBytes = hmacSHA256(kSigning, policyJson.toString().getBytes(StandardCharsets.UTF_8));
        String signatureHex = BaseEncoding.base16().lowerCase().encode(signatureBytes);

        System.out.println("Policy JSON:");
        System.out.println(policyJson);

        System.out.println("\nFull URL (for testing):");
        System.out.print("http://localhost:9444/" + bucket + "/" + key + "?");
        System.out.print("key=" + key);
        System.out.print("&policy=" + policyBase64);
        System.out.print("&x-amz-algorithm=AWS4-HMAC-SHA256");
        System.out.print("&x-amz-credential=" + credential);
        System.out.print("&x-amz-date=" + xAmzDate);
        System.out.print("&x-amz-security-token=" + sessionToken);
        System.out.println("&x-amz-signature=" + signatureHex);
    }

    private static byte[] hmacSHA256(byte[] key, String value) throws NoSuchAlgorithmException, InvalidKeyException {
        SecretKeySpec keySpec = new SecretKeySpec(key, "HmacSHA256");
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(keySpec);
        return mac.doFinal(value.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] hmacSHA256(byte[] key, byte[] value) throws NoSuchAlgorithmException, InvalidKeyException {
        SecretKeySpec keySpec = new SecretKeySpec(key, "HmacSHA256");
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(keySpec);
        return mac.doFinal(value);
    }
}
