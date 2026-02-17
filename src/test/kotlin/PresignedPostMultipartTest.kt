import com.google.common.io.BaseEncoding
import com.google.common.io.ByteStreams
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.kernel.SiriusExtension
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import java.net.HttpURLConnection
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

/**
 * Reproduces GitHub issue #287: "Missing file content in form data" on Presigned URL POST.
 * Sends a multipart/form-data POST to the bucket URL with a "file" part and verifies
 * the object is stored. Fails before the PresignedPostQueryProcessor fix (getFileData fallback),
 * passes after.
 */
@ExtendWith(SiriusExtension::class)
class PresignedPostMultipartTest {

    companion object {
        private const val ENDPOINT = "http://localhost:9999"
        private const val BUCKET = "presigned-post-test-bucket"
        private const val KEY = "57d5b61c-325c-4432-ba11-bfb0fa767561.txt"
        private const val FILE_CONTENT = "Hello, presigned POST multipart!"
        private const val BOUNDARY = "----WebKitFormBoundary7MA4YWxkTrZu0gW"
        private const val ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
        private const val SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        private const val REGION = "eu-central-1"
    }

    @Test
    fun `multipart form POST with file part uploads object successfully`() {
        // Create bucket first
        val client = createS3Client()
        client.use {
            it.createBucket { b -> b.bucket(BUCKET) }
        }

        // Generate presigned POST policy and signature
        val now = ZonedDateTime.now(ZoneId.of("UTC"))
        val date = DateTimeFormatter.ofPattern("yyyyMMdd").format(now)
        val xAmzDate = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").format(now)
        val expiration = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").format(now.plusHours(24))
        val credential = "$ACCESS_KEY/$date/$REGION/s3/aws4_request"

        val policyJson = """
            {
                "expiration": "$expiration",
                "conditions": [
                    {"bucket": "$BUCKET"},
                    {"key": "$KEY"},
                    {"x-amz-algorithm": "AWS4-HMAC-SHA256"},
                    {"x-amz-credential": "$credential"},
                    {"x-amz-date": "$xAmzDate"}
                ]
            }
        """.trimIndent()

        val policyBase64 = BaseEncoding.base64().encode(policyJson.toByteArray(StandardCharsets.UTF_8))
        val signature = calculateAwsSignature(SECRET_KEY, date, REGION, policyBase64)

        val postUrl = URI("$ENDPOINT/$BUCKET").toURL()
        val body = buildMultipartBody(KEY, FILE_CONTENT, policyBase64, signature, credential, xAmzDate)
        val bodyBytes = body.toByteArray(StandardCharsets.UTF_8)

        val connection = postUrl.openConnection() as HttpURLConnection
        connection.requestMethod = "POST"
        connection.doOutput = true
        connection.setRequestProperty(
            "Content-Type",
            "multipart/form-data; boundary=$BOUNDARY"
        )
        connection.setRequestProperty("Content-Length", bodyBytes.size.toString())

        connection.outputStream.use { out ->
            out.write(bodyBytes)
        }

        assertEquals(204, connection.responseCode, "Expected 204 No Content for presigned POST success")

        createS3Client().use {
            val response = it.getObject(GetObjectRequest.builder().bucket(BUCKET).key(KEY).build())
            assertNotNull(response)
            val downloaded = String(ByteStreams.toByteArray(response), StandardCharsets.UTF_8)
            assertEquals(FILE_CONTENT, downloaded, "Uploaded object content must match")

            // Cleanup
            it.deleteObject { d -> d.bucket(BUCKET).key(KEY) }
            it.deleteBucket { d -> d.bucket(BUCKET) }
        }
    }

    private fun buildMultipartBody(
        key: String,
        fileContent: String,
        policy: String,
        signature: String,
        credential: String,
        xAmzDate: String
    ): String {
        val crlf = "\r\n"
        return buildString {
            // Key field
            append("--$BOUNDARY$crlf")
            append("Content-Disposition: form-data; name=\"key\"$crlf$crlf")
            append("$key$crlf")
            // Algorithm
            append("--$BOUNDARY$crlf")
            append("Content-Disposition: form-data; name=\"x-amz-algorithm\"$crlf$crlf")
            append("AWS4-HMAC-SHA256$crlf")
            // Credential
            append("--$BOUNDARY$crlf")
            append("Content-Disposition: form-data; name=\"x-amz-credential\"$crlf$crlf")
            append("$credential$crlf")
            // Date
            append("--$BOUNDARY$crlf")
            append("Content-Disposition: form-data; name=\"x-amz-date\"$crlf$crlf")
            append("$xAmzDate$crlf")
            // Policy
            append("--$BOUNDARY$crlf")
            append("Content-Disposition: form-data; name=\"policy\"$crlf$crlf")
            append("$policy$crlf")
            // Signature
            append("--$BOUNDARY$crlf")
            append("Content-Disposition: form-data; name=\"x-amz-signature\"$crlf$crlf")
            append("$signature$crlf")
            // File (must be last per S3 spec)
            append("--$BOUNDARY$crlf")
            append("Content-Disposition: form-data; name=\"file\"; filename=\"test.txt\"$crlf")
            append("Content-Type: text/plain$crlf$crlf")
            append("$fileContent$crlf")
            append("--$BOUNDARY--$crlf")
        }
    }

    private fun createS3Client(): S3Client {
        return S3Client.builder()
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        ACCESS_KEY,
                        SECRET_KEY
                    )
                )
            )
            .serviceConfiguration(
                S3Configuration.builder()
                    .pathStyleAccessEnabled(true)
                    .build()
            )
            .endpointOverride(URI.create(ENDPOINT))
            .region(Region.EU_CENTRAL_1)
            .build()
    }

    private fun calculateAwsSignature(secretKey: String, date: String, region: String, policyBase64: String): String {
        val kSecret = "AWS4$secretKey".toByteArray(StandardCharsets.UTF_8)
        val kDate = hmacSHA256(kSecret, date)
        val kRegion = hmacSHA256(kDate, region)
        val kService = hmacSHA256(kRegion, "s3")
        val kSigning = hmacSHA256(kService, "aws4_request")
        val signatureBytes = hmacSHA256(kSigning, policyBase64)
        return BaseEncoding.base16().lowerCase().encode(signatureBytes)
    }

    private fun hmacSHA256(key: ByteArray, data: String): ByteArray {
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(key, "HmacSHA256"))
        return mac.doFinal(data.toByteArray(StandardCharsets.UTF_8))
    }
}
