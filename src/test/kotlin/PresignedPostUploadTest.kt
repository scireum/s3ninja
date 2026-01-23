/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

import com.google.common.io.BaseEncoding
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.kernel.SiriusExtension
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.HeadBucketRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import java.io.OutputStreamWriter
import java.io.PrintWriter
import java.net.HttpURLConnection
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Tests for presigned POST uploads using multipart/form-data.
 * This test reproduces the issue where s3ninja rejects valid multipart form uploads
 * with "Missing file content in form data" error.
 */
@ExtendWith(SiriusExtension::class)
class PresignedPostUploadTest {

    companion object {
        const val ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
        const val SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        const val ENDPOINT = "http://localhost:9999"
        const val REGION = "eu-central-1"
    }

    private fun getClient(): S3Client {
        return S3Client.builder()
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)
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

    @BeforeEach
    fun setup() {
        getClient().use { client ->
            client.listBuckets().buckets()?.forEach { bucket ->
                client.listObjects { it.bucket(bucket.name()) }.contents()?.forEach { obj ->
                    client.deleteObject(DeleteObjectRequest.builder().bucket(bucket.name()).key(obj.key()).build())
                }
                client.deleteBucket(DeleteBucketRequest.builder().bucket(bucket.name()).build())
            }
        }
    }

    /**
     * Test that reproduces the issue: multipart/form-data POST upload should work.
     * Before the fix, this test fails with "Missing file content in form data" error.
     */
    @Test
    fun `multipart form-data POST upload works as expected`() {
        val bucketName = "test-bucket"
        val objectKey = "${UUID.randomUUID()}.txt"
        val fileContent = "Hello, this is test content for presigned POST upload!"

        // Create bucket first
        getClient().use { client ->
            client.createBucket { it.bucket(bucketName) }
            assertTrue(doesBucketExist(client, bucketName))
        }

        // Generate presigned POST policy and signature
        val now = ZonedDateTime.now(ZoneId.of("UTC"))
        val date = DateTimeFormatter.ofPattern("yyyyMMdd").format(now)
        val xAmzDate = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").format(now)
        val expiration = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").format(now.plusHours(24))
        val credential = "$ACCESS_KEY/$date/$REGION/s3/aws4_request"

        // Build policy JSON
        val policyJson = """
            {
                "expiration": "$expiration",
                "conditions": [
                    {"bucket": "$bucketName"},
                    {"key": "$objectKey"},
                    {"x-amz-algorithm": "AWS4-HMAC-SHA256"},
                    {"x-amz-credential": "$credential"},
                    {"x-amz-date": "$xAmzDate"}
                ]
            }
        """.trimIndent()

        val policyBase64 = BaseEncoding.base64().encode(policyJson.toByteArray(StandardCharsets.UTF_8))
        val signature = calculateAwsSignature(SECRET_KEY, date, REGION, policyJson.toByteArray(StandardCharsets.UTF_8))

        // Send multipart/form-data POST request
        val boundary = "----WebKitFormBoundary${UUID.randomUUID().toString().replace("-", "").take(16)}"
        val url = URI.create("$ENDPOINT/$bucketName").toURL()

        val connection = url.openConnection() as HttpURLConnection
        connection.requestMethod = "POST"
        connection.doOutput = true
        connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=$boundary")

        connection.outputStream.use { outputStream ->
            PrintWriter(OutputStreamWriter(outputStream, StandardCharsets.UTF_8), true).use { writer ->
                // Add form fields (order matters: file must be last per S3 spec)
                addFormField(writer, boundary, "key", objectKey)
                addFormField(writer, boundary, "x-amz-algorithm", "AWS4-HMAC-SHA256")
                addFormField(writer, boundary, "x-amz-credential", credential)
                addFormField(writer, boundary, "x-amz-date", xAmzDate)
                addFormField(writer, boundary, "policy", policyBase64)
                addFormField(writer, boundary, "x-amz-signature", signature)

                // Add file field (must be last)
                writer.append("--$boundary\r\n")
                writer.append("Content-Disposition: form-data; name=\"file\"; filename=\"test.txt\"\r\n")
                writer.append("Content-Type: text/plain\r\n")
                writer.append("\r\n")
                writer.append(fileContent)
                writer.append("\r\n")

                // End of multipart
                writer.append("--$boundary--\r\n")
                writer.flush()
            }
        }

        val responseCode = connection.responseCode
        val responseBody = if (responseCode >= 400) {
            connection.errorStream?.bufferedReader()?.readText() ?: ""
        } else {
            connection.inputStream?.bufferedReader()?.readText() ?: ""
        }

        assertEquals(
            204,
            responseCode,
            "Expected 204 but got $responseCode. Response: $responseBody"
        )

        // Verify object was created
        getClient().use { client ->
            val getResponse = client.getObject { it.bucket(bucketName).key(objectKey) }
            val downloadedContent = String(getResponse.readAllBytes(), StandardCharsets.UTF_8)
            assertEquals(fileContent, downloadedContent, "Downloaded content should match uploaded content")

            // Cleanup
            client.deleteObject { it.bucket(bucketName).key(objectKey) }
            client.deleteBucket { it.bucket(bucketName) }
        }
    }

    /**
     * Test that empty file uploads also work via multipart form-data.
     */
    @Test
    fun `multipart form-data POST upload with empty file works as expected`() {
        val bucketName = "test-bucket"
        val objectKey = "${UUID.randomUUID()}.txt"

        // Create bucket first
        getClient().use { client ->
            client.createBucket { it.bucket(bucketName) }
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
                    {"bucket": "$bucketName"},
                    {"key": "$objectKey"},
                    {"x-amz-algorithm": "AWS4-HMAC-SHA256"},
                    {"x-amz-credential": "$credential"},
                    {"x-amz-date": "$xAmzDate"}
                ]
            }
        """.trimIndent()

        val policyBase64 = BaseEncoding.base64().encode(policyJson.toByteArray(StandardCharsets.UTF_8))
        val signature = calculateAwsSignature(SECRET_KEY, date, REGION, policyJson.toByteArray(StandardCharsets.UTF_8))

        val boundary = "----WebKitFormBoundary${UUID.randomUUID().toString().replace("-", "").take(16)}"
        val url = URI.create("$ENDPOINT/$bucketName").toURL()

        val connection = url.openConnection() as HttpURLConnection
        connection.requestMethod = "POST"
        connection.doOutput = true
        connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=$boundary")

        connection.outputStream.use { outputStream ->
            PrintWriter(OutputStreamWriter(outputStream, StandardCharsets.UTF_8), true).use { writer ->
                addFormField(writer, boundary, "key", objectKey)
                addFormField(writer, boundary, "x-amz-algorithm", "AWS4-HMAC-SHA256")
                addFormField(writer, boundary, "x-amz-credential", credential)
                addFormField(writer, boundary, "x-amz-date", xAmzDate)
                addFormField(writer, boundary, "policy", policyBase64)
                addFormField(writer, boundary, "x-amz-signature", signature)

                // Add empty file field
                writer.append("--$boundary\r\n")
                writer.append("Content-Disposition: form-data; name=\"file\"; filename=\"empty.txt\"\r\n")
                writer.append("Content-Type: text/plain\r\n")
                writer.append("\r\n")
                // No content - empty file
                writer.append("\r\n")

                writer.append("--$boundary--\r\n")
                writer.flush()
            }
        }

        val responseCode = connection.responseCode
        val responseBody = if (responseCode >= 400) {
            connection.errorStream?.bufferedReader()?.readText() ?: ""
        } else {
            connection.inputStream?.bufferedReader()?.readText() ?: ""
        }

        assertEquals(
            204,
            responseCode,
            "Expected 204 but got $responseCode. Response: $responseBody"
        )

        // Verify empty object was created
        getClient().use { client ->
            val getResponse = client.getObject { it.bucket(bucketName).key(objectKey) }
            val downloadedContent = String(getResponse.readAllBytes(), StandardCharsets.UTF_8)
            assertEquals("", downloadedContent, "Downloaded content should be empty")

            // Cleanup
            client.deleteObject { it.bucket(bucketName).key(objectKey) }
            client.deleteBucket { it.bucket(bucketName) }
        }
    }

    private fun addFormField(writer: PrintWriter, boundary: String, name: String, value: String) {
        writer.append("--$boundary\r\n")
        writer.append("Content-Disposition: form-data; name=\"$name\"\r\n")
        writer.append("\r\n")
        writer.append(value)
        writer.append("\r\n")
    }

    private fun calculateAwsSignature(secretKey: String, date: String, region: String, policyBytes: ByteArray): String {
        val kSecret = "AWS4$secretKey".toByteArray(StandardCharsets.UTF_8)
        val kDate = hmacSHA256(kSecret, date)
        val kRegion = hmacSHA256(kDate, region)
        val kService = hmacSHA256(kRegion, "s3")
        val kSigning = hmacSHA256(kService, "aws4_request")

        val signatureBytes = hmacSHA256(kSigning, policyBytes)
        return BaseEncoding.base16().lowerCase().encode(signatureBytes)
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

    private fun doesBucketExist(client: S3Client, bucketName: String): Boolean {
        return try {
            client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build())
            true
        } catch (_: S3Exception) {
            false
        }
    }
}
