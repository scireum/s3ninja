import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.common.io.BaseEncoding
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.kernel.SiriusExtension
import java.nio.charset.StandardCharsets
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Tests for generating presigned POST policies and signatures using Jackson for JSON construction.
 * This replaces the manual string concatenation approach with proper JSON building.
 */
@ExtendWith(SiriusExtension::class)
class PresignedPostGeneratorTest {

    private val objectMapper = ObjectMapper()

    @Test
    fun `generate presigned POST with session token`() {
        val bucket = "test"
        val key = "okok.txt"
        val region = "eu-west-3"
        val accessKey = "AKIAIOSFODNN7EXAMPLE"
        val secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        val sessionToken = "047b688c0be3e87dcda3e07"

        val now = ZonedDateTime.now(ZoneId.of("UTC"))
        val date = DateTimeFormatter.ofPattern("yyyyMMdd").format(now)
        val xAmzDate = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").format(now)
        val expiration = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").format(now.plusHours(24))
        val credential = "$accessKey/$date/$region/s3/aws4_request"

        // Build Policy JSON using Jackson
        val policy = buildPresignedPostPolicy(bucket, key, credential, xAmzDate, expiration, sessionToken)
        val policyJson = objectMapper.writeValueAsString(policy)
        val policyBase64 = BaseEncoding.base64().encode(policyJson.toByteArray(StandardCharsets.UTF_8))

        // Calculate Signature (AWS Signature Version 4)
        val signature = calculateAwsSignature(secretKey, date, region, policyJson.toByteArray(StandardCharsets.UTF_8))

        // Build the presigned POST URL
        val url = buildPresignedPostUrl(bucket, key, policyBase64, credential, xAmzDate, sessionToken, signature)

        // Assertions
        assertNotNull(policyJson)
        assertNotNull(signature)
        assertNotNull(url)
        assertTrue(url.contains("x-amz-signature=$signature"))
        assertTrue(url.contains("x-amz-security-token=$sessionToken"))

        // Print for manual testing if needed
        println("Policy JSON:")
        println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(policy))
        println("\nBase64 Policy:")
        println(policyBase64)
        println("\nSignature:")
        println(signature)
        println("\nFull URL:")
        println(url)
    }

    @Test
    fun `generate presigned POST without session token`() {
        val bucket = "test-bucket"
        val key = "test-file.txt"
        val region = "us-east-1"
        val accessKey = "AKIAIOSFODNN7EXAMPLE"
        val secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

        val now = ZonedDateTime.now(ZoneId.of("UTC"))
        val date = DateTimeFormatter.ofPattern("yyyyMMdd").format(now)
        val xAmzDate = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").format(now)
        val expiration = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").format(now.plusHours(24))
        val credential = "$accessKey/$date/$region/s3/aws4_request"

        // Build Policy JSON using Jackson (no session token)
        val policy = buildPresignedPostPolicy(bucket, key, credential, xAmzDate, expiration, null)
        val policyJson = objectMapper.writeValueAsString(policy)
        val policyBase64 = BaseEncoding.base64().encode(policyJson.toByteArray(StandardCharsets.UTF_8))

        // Calculate Signature
        val signature = calculateAwsSignature(secretKey, date, region, policyJson.toByteArray(StandardCharsets.UTF_8))

        // Build URL without session token
        val url = buildPresignedPostUrl(bucket, key, policyBase64, credential, xAmzDate, null, signature)

        // Assertions
        assertNotNull(policyJson)
        assertNotNull(signature)
        assertNotNull(url)
        assertTrue(url.contains("x-amz-signature=$signature"))
        assertTrue(!url.contains("x-amz-security-token"))
    }

    /**
     * Builds a presigned POST policy using Jackson for proper JSON construction.
     */
    private fun buildPresignedPostPolicy(
        bucket: String,
        key: String,
        credential: String,
        xAmzDate: String,
        expiration: String,
        sessionToken: String?
    ): ObjectNode {
        val policy = objectMapper.createObjectNode()
        policy.put("expiration", expiration)

        val conditions = objectMapper.createArrayNode()

        // Add bucket condition
        conditions.add(objectMapper.createObjectNode().put("bucket", bucket))

        // Add key condition with exact match
        val keyCondition = objectMapper.createArrayNode()
        keyCondition.add("eq")
        keyCondition.add("\$key")
        keyCondition.add(key)
        conditions.add(keyCondition)

        // Add AWS SigV4 required fields
        conditions.add(objectMapper.createObjectNode().put("x-amz-algorithm", "AWS4-HMAC-SHA256"))
        conditions.add(objectMapper.createObjectNode().put("x-amz-credential", credential))
        conditions.add(objectMapper.createObjectNode().put("x-amz-date", xAmzDate))

        // Add session token if provided
        if (sessionToken != null) {
            conditions.add(objectMapper.createObjectNode().put("x-amz-security-token", sessionToken))
        }

        policy.set<ArrayNode>("conditions", conditions)
        return policy
    }

    /**
     * Calculates AWS Signature Version 4 for the policy.
     */
    private fun calculateAwsSignature(
        secretKey: String,
        date: String,
        region: String,
        policyBytes: ByteArray
    ): String {
        val kSecret = "AWS4$secretKey".toByteArray(StandardCharsets.UTF_8)
        val kDate = hmacSHA256(kSecret, date)
        val kRegion = hmacSHA256(kDate, region)
        val kService = hmacSHA256(kRegion, "s3")
        val kSigning = hmacSHA256(kService, "aws4_request")

        // Sign the policy document bytes (not the base64 string)
        val signatureBytes = hmacSHA256(kSigning, policyBytes)
        return BaseEncoding.base16().lowerCase().encode(signatureBytes)
    }

    /**
     * Builds the full presigned POST URL with all required parameters.
     */
    private fun buildPresignedPostUrl(
        bucket: String,
        key: String,
        policyBase64: String,
        credential: String,
        xAmzDate: String,
        sessionToken: String?,
        signature: String
    ): String {
        val params = mutableListOf(
            "key=$key",
            "policy=$policyBase64",
            "x-amz-algorithm=AWS4-HMAC-SHA256",
            "x-amz-credential=$credential",
            "x-amz-date=$xAmzDate"
        )

        if (sessionToken != null) {
            params.add("x-amz-security-token=$sessionToken")
        }

        params.add("x-amz-signature=$signature")

        return "http://localhost:9444/$bucket/$key?" + params.joinToString("&")
    }

    private fun hmacSHA256(key: ByteArray, value: String): ByteArray {
        return hmacSHA256(key, value.toByteArray(StandardCharsets.UTF_8))
    }

    private fun hmacSHA256(key: ByteArray, value: ByteArray): ByteArray {
        val keySpec = SecretKeySpec(key, "HmacSHA256")
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(keySpec)
        return mac.doFinal(value)
    }
}

