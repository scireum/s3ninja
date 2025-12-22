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

/**
 * Generates a presigned POST policy and signature for testing.
 */
public class PresignedPostGenerator {

    public static void main(String[] args) throws Exception {
        String bucket = "test";
        String key = "okok.txt";
        String region = "eu-west-3";
        String accessKey = "AKIAIOSFODNN7=EXAMPLE";
        String secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

        // TODO: Paste your session token here if testing with temporary credentials
        String sessionToken = "";

        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
        String date = DateTimeFormatter.ofPattern("yyyyMMdd").format(now);
        String xAmzDate = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").format(now);

        // Expiration: 24 hours from now
        String expiration = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").format(now.plusHours(24));

        String credential = accessKey + "/" + date + "/" + region + "/s3/aws4_request";

        // Build Policy JSON
        StringBuilder policyJson = new StringBuilder();
        policyJson.append("{\n");
        policyJson.append("  \"expiration\": \"" + expiration + "\",\n");
        policyJson.append("  \"conditions\": [\n");
        policyJson.append("    {\"bucket\": \"" + bucket + "\"},\n");
        policyJson.append("    [\"eq\", \"$key\", \"" + key + "\"],\n");
        policyJson.append("    {\"x-amz-algorithm\": \"AWS4-HMAC-SHA256\"},\n");
        policyJson.append("    {\"x-amz-credential\": \"" + credential + "\"},");
        policyJson.append("    {\"x-amz-date\": \"" + xAmzDate + "\"}");

        if (sessionToken != null && !sessionToken.isEmpty()) {
            policyJson.append(",\n    {\"x-amz-security-token\": \"" + sessionToken + "\"}");
        }

        policyJson.append("\n  ]\n");
        policyJson.append("}");

        String policyBase64 = BaseEncoding.base64().encode(policyJson.toString().getBytes(StandardCharsets.UTF_8));

        // Calculate Signature
        byte[] kSecret = ("AWS4" + secretKey).getBytes(StandardCharsets.UTF_8);
        byte[] kDate = hmacSHA256(kSecret, date);
        byte[] kRegion = hmacSHA256(kDate, region);
        byte[] kService = hmacSHA256(kRegion, "s3");
        byte[] kSigning = hmacSHA256(kService, "aws4_request");
        byte[] signatureBytes = hmacSHA256(kSigning, policyBase64);
        String signatureHex = BaseEncoding.base16().lowerCase().encode(signatureBytes);

        System.out.println("Policy JSON:");
        System.out.println(policyJson);
        System.out.println("\nGenerated Parameters:");
        System.out.println("key=" + key);
        System.out.println("policy=" + policyBase64);
        System.out.println("x-amz-algorithm=AWS4-HMAC-SHA256");
        System.out.println("x-amz-credential=" + credential);
        System.out.println("x-amz-date=" + xAmzDate);
        if (sessionToken != null && !sessionToken.isEmpty()) {
            System.out.println("x-amz-security-token=" + sessionToken);
        }
        System.out.println("x-amz-signature=" + signatureHex);

        System.out.println("\nFull URL (for testing):");
        System.out.print("http://localhost:9444/" + bucket + "/" + key + "?");
        System.out.print("key=" + key);
        System.out.print("&policy=" + policyBase64);
        System.out.print("&x-amz-algorithm=AWS4-HMAC-SHA256");
        System.out.print("&x-amz-credential=" + credential);
        System.out.print("&x-amz-date=" + xAmzDate);
        if (sessionToken != null && !sessionToken.isEmpty()) {
            System.out.print("&x-amz-security-token=" + sessionToken);
        }
        System.out.println("&x-amz-signature=" + signatureHex);
    }

    private static byte[] hmacSHA256(byte[] key, String value) throws NoSuchAlgorithmException, InvalidKeyException {
        SecretKeySpec keySpec = new SecretKeySpec(key, "HmacSHA256");
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(keySpec);
        return mac.doFinal(value.getBytes(StandardCharsets.UTF_8));
    }
}
