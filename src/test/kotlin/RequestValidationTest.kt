/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

import BaseSdkSupportTest.Companion.assertS3Error
import BaseSdkSupportTest.Companion.cleanupBuckets
import BaseSdkSupportTest.Companion.createBucket
import BaseSdkSupportTest.Companion.getPresigner
import BaseSdkSupportTest.Companion.putObjectWithContent
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.kernel.SiriusExtension
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration
import java.net.HttpURLConnection
import java.net.URI
import java.time.Duration
import kotlin.test.assertEquals

/**
 * Verifies that requests are checked as strictly as S3 checks them, apart from multipart uploads which are covered by
 * [BaseSdkSupportTest].
 */
@ExtendWith(SiriusExtension::class)
class RequestValidationTest {

    @Test
    fun `Bucket queries on a private bucket require a signature`() {
        createClient().use { client ->
            createBucket(client, BUCKET)
            putObjectWithContent(client, BUCKET, KEY, "Test")

            val deleteStatus = sendUnsigned(
                "POST", "/$BUCKET?delete", "<Delete><Object><Key>$KEY</Key></Object></Delete>"
            )
            assertEquals(403, deleteStatus)
            assertEquals(403, sendUnsigned("GET", "/$BUCKET?location"))
            assertEquals(403, sendUnsigned("GET", "/$BUCKET?acl"))

            assertEquals(listOf(KEY), client.listObjectsV2 { it.bucket(BUCKET) }.contents().map { it.key() })
            client.getBucketLocation { it.bucket(BUCKET) }

            cleanupBuckets(client, BUCKET)
        }
    }

    @Test
    fun `Request signed for an unknown access key is refused`() {
        createClient().use { client ->
            createBucket(client, BUCKET)

            createClient(accessKey = "AKIAUNKNOWNEXAMPLE").use { strangerClient ->
                assertS3Error("InvalidAccessKeyId") { strangerClient.listObjectsV2 { it.bucket(BUCKET) } }
            }

            cleanupBuckets(client, BUCKET)
        }
    }

    @Test
    fun `Expired presigned URL is refused`() {
        createClient().use { client ->
            createBucket(client, BUCKET)
            putObjectWithContent(client, BUCKET, KEY, "Test")

            getPresigner().use { presigner ->
                val shortLivedUrl = presigner.presignGetObject { builder ->
                    builder.getObjectRequest { it.bucket(BUCKET).key(KEY) }.signatureDuration(Duration.ofSeconds(1))
                }.url()
                val longLivedUrl = presigner.presignGetObject { builder ->
                    builder.getObjectRequest { it.bucket(BUCKET).key(KEY) }.signatureDuration(Duration.ofMinutes(10))
                }.url()

                Thread.sleep(2500)

                assertEquals(403, (shortLivedUrl.openConnection() as HttpURLConnection).responseCode)
                assertEquals(200, (longLivedUrl.openConnection() as HttpURLConnection).responseCode)

                // S3 permits a week at most, which is checked before the signature is looked at.
                val validForTooLong =
                    longLivedUrl.toString().replace(Regex("X-Amz-Expires=\\d+"), "X-Amz-Expires=604801")
                assertEquals(400, (URI(validForTooLong).toURL().openConnection() as HttpURLConnection).responseCode)
            }

            cleanupBuckets(client, BUCKET)
        }
    }

    /**
     * Starts every test without any bucket, so that tests neither depend on nor disturb each other.
     */
    @BeforeEach
    fun removeAllBuckets() {
        createClient().use { client ->
            client.listBuckets().buckets().forEach { cleanupBuckets(client, it.name()) }
        }
    }

    private fun createClient(accessKey: String = BaseSdkSupportTest.ACCESS_KEY): S3Client {
        return S3Client.builder()
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, BaseSdkSupportTest.SECRET_ACCESS_KEY)
                )
            )
            .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
            .endpointOverride(URI.create(BaseSdkSupportTest.ENDPOINT))
            .region(Region.EU_CENTRAL_1)
            .build()
    }

    /**
     * Sends a request without any signature and returns the status code of the response.
     */
    private fun sendUnsigned(method: String, path: String, xmlBody: String? = null): Int {
        val connection = URI(BaseSdkSupportTest.ENDPOINT + path).toURL().openConnection() as HttpURLConnection
        connection.requestMethod = method
        if (xmlBody != null) {
            connection.doOutput = true
            connection.setRequestProperty("Content-Type", "application/xml")
            connection.outputStream.use { it.write(xmlBody.toByteArray()) }
        }
        return connection.responseCode.also { connection.disconnect() }
    }

    companion object {
        const val BUCKET = "validation"
        const val KEY = "some/key"
    }
}
