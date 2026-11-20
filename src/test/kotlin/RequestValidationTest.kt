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
import BaseSdkSupportTest.Companion.md5
import BaseSdkSupportTest.Companion.putObjectWithContent
import com.google.common.io.BaseEncoding
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import ninja.Storage
import sirius.kernel.SiriusExtension
import sirius.kernel.di.Injector
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.services.s3.model.BucketCannedACL
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload
import software.amazon.awssdk.services.s3.model.CompletedPart
import software.amazon.awssdk.services.s3.model.S3Exception
import java.net.HttpURLConnection
import java.net.URI
import java.security.MessageDigest
import java.time.Duration
import java.time.Instant
import java.util.Base64
import kotlin.test.assertEquals
import kotlin.test.assertTrue

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

    @Test
    fun `PutObject with a wrong Content-MD5 or checksum is refused and keeps the previous object`() {
        createClient().use { client ->
            createBucket(client, BUCKET)
            putObjectWithContent(client, BUCKET, KEY, "Previous")
            val data = "Test".toByteArray()

            assertS3Error("BadDigest") {
                client.putObject(
                    { it.bucket(BUCKET).key(KEY).contentMD5(base64(md5("Other".toByteArray()))) },
                    RequestBody.fromBytes(data)
                )
            }
            assertS3Error("InvalidDigest") {
                client.putObject({ it.bucket(BUCKET).key(KEY).contentMD5("AAAA") }, RequestBody.fromBytes(data))
            }
            assertS3Error("BadDigest") {
                client.putObject(
                    { it.bucket(BUCKET).key(KEY).checksumSHA256(base64(sha256("Other".toByteArray()))) },
                    RequestBody.fromBytes(data)
                )
            }
            assertS3Error("BadDigest") {
                client.putObject({ it.bucket(BUCKET).key(KEY).checksumCRC32("AAAAAA==") }, RequestBody.fromBytes(data))
            }
            assertEquals("Previous", client.getObject { it.bucket(BUCKET).key(KEY) }.use { String(it.readAllBytes()) })

            client.putObject(
                {
                    it.bucket(BUCKET).key(KEY).contentMD5(base64(md5(data))).checksumSHA256(base64(sha256(data)))
                },
                RequestBody.fromBytes(data)
            )
            assertEquals("Test", client.getObject { it.bucket(BUCKET).key(KEY) }.use { String(it.readAllBytes()) })

            cleanupBuckets(client, BUCKET)
        }
    }

    @Test
    fun `PutObject with a wrong x-amz-content-sha256 is refused`() {
        createClient().use { client ->
            client.createBucket { it.bucket(BUCKET).acl(BucketCannedACL.PUBLIC_READ_WRITE) }
            val data = "Test".toByteArray()

            val wrongHash = BaseEncoding.base16().lowerCase().encode(sha256("Other".toByteArray()))
            assertEquals(400, sendUnsigned("PUT", "/$BUCKET/$KEY", "Test", mapOf("x-amz-content-sha256" to wrongHash)))
            assertS3Error("NoSuchKey") { client.headObject { it.bucket(BUCKET).key(KEY) } }
            assertEquals(
                400, sendUnsigned("PUT", "/$BUCKET/$KEY", "Test", mapOf("x-amz-content-sha256" to "not-a-hash"))
            )
            assertEquals(
                400,
                sendUnsigned("PUT", "/$BUCKET/$KEY", "Test", mapOf("x-amz-content-sha256" to "STREAMING-UNKNOWN"))
            )
            assertS3Error("NoSuchKey") { client.headObject { it.bucket(BUCKET).key(KEY) } }

            val hash = BaseEncoding.base16().lowerCase().encode(sha256(data))
            assertEquals(200, sendUnsigned("PUT", "/$BUCKET/$KEY", "Test", mapOf("x-amz-content-sha256" to hash)))
            assertEquals(
                200, sendUnsigned("PUT", "/$BUCKET/$KEY", "Test", mapOf("x-amz-content-sha256" to "UNSIGNED-PAYLOAD"))
            )

            cleanupBuckets(client, BUCKET)
        }
    }

    @Test
    fun `UploadPart with a wrong checksum is refused`() {
        createClient().use { client ->
            createBucket(client, BUCKET)
            val uploadId = client.createMultipartUpload { it.bucket(BUCKET).key(KEY) }.uploadId()

            assertS3Error("BadDigest") {
                client.uploadPart(
                    { it.bucket(BUCKET).key(KEY).uploadId(uploadId).partNumber(1).checksumCRC32("AAAAAA==") },
                    RequestBody.fromBytes("Test".toByteArray())
                )
            }
            assertTrue(client.listParts { it.bucket(BUCKET).key(KEY).uploadId(uploadId) }.parts().isEmpty())

            cleanupBuckets(client, BUCKET)
        }
    }

    @Test
    fun `GetObject and HeadObject honour conditions`() {
        createClient().use { client ->
            createBucket(client, BUCKET)
            putObjectWithContent(client, BUCKET, KEY, "Test")
            val etag = client.headObject { it.bucket(BUCKET).key(KEY) }.eTag()
            val past = Instant.now().minus(Duration.ofDays(1))
            val future = Instant.now().plus(Duration.ofDays(1))

            assertStatus(412) { client.headObject { it.bucket(BUCKET).key(KEY).ifMatch("\"other\"") } }
            assertStatus(412) { client.getObject { it.bucket(BUCKET).key(KEY).ifMatch("\"other\"") } }

            // If-Match compares to the letter and never matches a tag marked as weak, unlike If-None-Match.
            assertStatus(412) { client.headObject { it.bucket(BUCKET).key(KEY).ifMatch("W/$etag") } }
            assertStatus(412) { client.headObject { it.bucket(BUCKET).key(KEY).ifMatch(etag.uppercase()) } }
            assertStatus(304) { client.headObject { it.bucket(BUCKET).key(KEY).ifNoneMatch("W/$etag") } }

            // Early versions stored entity tags in upper case. They are sent in lower case, which has to match.
            val storedObject = Injector.context().getPart(Storage::class.java)!!.getBucket(BUCKET).getObject(KEY)
            storedObject.properties = mapOf("ETag" to etag.trim('"').uppercase())
            assertEquals(etag, client.headObject { it.bucket(BUCKET).key(KEY) }.eTag())
            client.headObject { it.bucket(BUCKET).key(KEY).ifMatch(etag) }
            assertStatus(304) { client.headObject { it.bucket(BUCKET).key(KEY).ifNoneMatch(etag) } }
            assertStatus(304) { client.getObject { it.bucket(BUCKET).key(KEY).ifNoneMatch(etag) } }
            assertStatus(412) { client.headObject { it.bucket(BUCKET).key(KEY).ifUnmodifiedSince(past) } }
            assertStatus(304) { client.headObject { it.bucket(BUCKET).key(KEY).ifModifiedSince(future) } }

            client.headObject { it.bucket(BUCKET).key(KEY).ifMatch(etag).ifUnmodifiedSince(past) }
            client.headObject { it.bucket(BUCKET).key(KEY).ifNoneMatch("\"other\"").ifModifiedSince(future) }
            client.getObject { it.bucket(BUCKET).key(KEY).ifMatch(etag) }.close()

            cleanupBuckets(client, BUCKET)
        }
    }

    @Test
    fun `PutObject honours conditions and keeps the object if they do not hold`() {
        createClient().use { client ->
            createBucket(client, BUCKET)
            val body = RequestBody.fromBytes("Changed".toByteArray())

            client.putObject(
                { it.bucket(BUCKET).key(KEY).ifNoneMatch("*") },
                RequestBody.fromBytes("Test".toByteArray())
            )
            val etag = client.headObject { it.bucket(BUCKET).key(KEY) }.eTag()

            assertS3Error("PreconditionFailed") {
                client.putObject(
                    { it.bucket(BUCKET).key(KEY).ifNoneMatch("*") },
                    body
                )
            }
            assertS3Error("PreconditionFailed") {
                client.putObject({ it.bucket(BUCKET).key(KEY).ifMatch("\"other\"") }, body)
            }
            assertEquals("Test", client.getObject { it.bucket(BUCKET).key(KEY) }.use { String(it.readAllBytes()) })

            client.putObject({ it.bucket(BUCKET).key(KEY).ifMatch(etag) }, body)
            assertEquals("Changed", client.getObject { it.bucket(BUCKET).key(KEY) }.use { String(it.readAllBytes()) })

            cleanupBuckets(client, BUCKET)
        }
    }

    @Test
    fun `CompleteMultipartUpload honours If-None-Match`() {
        createClient().use { client ->
            createBucket(client, BUCKET)
            putObjectWithContent(client, BUCKET, KEY, "Test")

            val uploadId = client.createMultipartUpload { it.bucket(BUCKET).key(KEY) }.uploadId()
            val part = client.uploadPart(
                { it.bucket(BUCKET).key(KEY).uploadId(uploadId).partNumber(1) },
                RequestBody.fromBytes("Changed".toByteArray())
            )
            val parts = CompletedMultipartUpload.builder()
                .parts(CompletedPart.builder().partNumber(1).eTag(part.eTag()).build())
                .build()

            assertS3Error("PreconditionFailed") {
                client.completeMultipartUpload {
                    it.bucket(BUCKET).key(KEY).uploadId(uploadId).multipartUpload(parts).ifNoneMatch("*")
                }
            }
            assertEquals("Test", client.getObject { it.bucket(BUCKET).key(KEY) }.use { String(it.readAllBytes()) })

            client.completeMultipartUpload { it.bucket(BUCKET).key(KEY).uploadId(uploadId).multipartUpload(parts) }

            cleanupBuckets(client, BUCKET)
        }
    }

    @Test
    fun `CopyObject honours conditions on its source`() {
        createClient().use { client ->
            createBucket(client, BUCKET)
            putObjectWithContent(client, BUCKET, KEY, "Test")
            val etag = client.headObject { it.bucket(BUCKET).key(KEY) }.eTag()

            assertS3Error("PreconditionFailed") {
                client.copyObject {
                    it.sourceBucket(BUCKET).sourceKey(KEY).destinationBucket(BUCKET).destinationKey("copy")
                        .copySourceIfMatch("\"other\"")
                }
            }
            assertS3Error("PreconditionFailed") {
                client.copyObject {
                    it.sourceBucket(BUCKET).sourceKey(KEY).destinationBucket(BUCKET).destinationKey("copy")
                        .copySourceIfNoneMatch(etag)
                }
            }
            assertS3Error("NoSuchKey") { client.headObject { it.bucket(BUCKET).key("copy") } }

            client.copyObject {
                it.sourceBucket(BUCKET).sourceKey(KEY).destinationBucket(BUCKET).destinationKey("copy")
                    .copySourceIfMatch(etag)
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
    private fun sendUnsigned(
        method: String,
        path: String,
        body: String? = null,
        headers: Map<String, String> = emptyMap()
    ): Int {
        val connection = URI(BaseSdkSupportTest.ENDPOINT + path).toURL().openConnection() as HttpURLConnection
        connection.requestMethod = method
        connection.setRequestProperty("Content-Type", "application/xml")
        headers.forEach { (name, value) -> connection.setRequestProperty(name, value) }
        if (body != null) {
            connection.doOutput = true
            connection.outputStream.use { it.write(body.toByteArray()) }
        }
        return connection.responseCode.also { connection.disconnect() }
    }

    /**
     * Asserts that the given block is answered with the given HTTP status.
     */
    private fun assertStatus(expectedStatus: Int, block: () -> Unit) {
        val exception = assertThrows<S3Exception> { block() }
        assertEquals(expectedStatus, exception.statusCode())
    }

    private fun sha256(data: ByteArray): ByteArray {
        return MessageDigest.getInstance("SHA-256").digest(data)
    }

    private fun base64(data: ByteArray): String {
        return Base64.getEncoder().encodeToString(data)
    }

    companion object {
        const val BUCKET = "validation"
        const val KEY = "some/key"
    }
}
