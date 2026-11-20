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
import software.amazon.awssdk.services.s3.model.CORSRule
import software.amazon.awssdk.services.s3.model.ChecksumMode
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload
import software.amazon.awssdk.services.s3.model.CompletedPart
import software.amazon.awssdk.services.s3.model.EncodingType
import software.amazon.awssdk.services.s3.model.MetadataDirective
import software.amazon.awssdk.services.s3.model.ObjectIdentifier
import software.amazon.awssdk.services.s3.model.Payer
import software.amazon.awssdk.services.s3.model.Permission
import software.amazon.awssdk.services.s3.model.S3Exception
import java.io.ByteArrayOutputStream
import java.net.HttpURLConnection
import java.net.URI
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Duration
import java.time.Instant
import java.util.Base64
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPOutputStream
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
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
    fun `ListBuckets is served without a signature, but refuses a wrong one`() {
        assertEquals(200, sendUnsigned("GET", "/"))
        assertEquals(400, sendUnsigned("GET", "/", null, mapOf("Authorization" to "AWS4-HMAC-SHA256 nonsense")))
        createClient().use { client -> client.listBuckets() }
        createClient("AKIAUNKNOWNACCESSKEY").use { client ->
            assertS3Error("InvalidAccessKeyId") { client.listBuckets() }
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
                val withoutExpiry = longLivedUrl.toString().replace(Regex("X-Amz-Expires=\\d+"), "X-Amz-Expires=")
                assertEquals(400, (URI(withoutExpiry).toURL().openConnection() as HttpURLConnection).responseCode)
                val negativeExpiry = longLivedUrl.toString().replace(Regex("X-Amz-Expires=\\d+"), "X-Amz-Expires=-1")
                assertEquals(400, (URI(negativeExpiry).toURL().openConnection() as HttpURLConnection).responseCode)

                // A URL signed with signature version 2 has to name its access key and the time it expires at.
                assertEquals(403, sendUnsigned("GET", "/$BUCKET/$KEY?Signature=x&Expires=4102444800"))
                assertEquals(
                    403,
                    sendUnsigned("GET", "/$BUCKET/$KEY?Signature=x&AWSAccessKeyId=${BaseSdkSupportTest.ACCESS_KEY}")
                )
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
            client.getObject { it.bucket(BUCKET).key(KEY).ifNoneMatch("\"other\"").ifModifiedSince(future) }.close()

            // Conditions are decided before the range is looked at.
            assertStatus(304) {
                client.getObject { it.bucket(BUCKET).key(KEY).ifNoneMatch(etag).range("bytes=100-") }
            }

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
    fun `Of several requests creating the same object only if it does not exist, exactly one succeeds`() {
        createClient().use { client ->
            createBucket(client, BUCKET)

            // Large enough for all requests to be on their way before the first one has been received completely.
            val content = ByteArray(2 * 1024 * 1024)
            val attempts = 8
            val start = CountDownLatch(1)
            val executor = Executors.newFixedThreadPool(attempts)
            val outcomes = (1..attempts).map {
                executor.submit<Boolean> {
                    start.await()
                    try {
                        client.putObject(
                            { it.bucket(BUCKET).key(KEY).ifNoneMatch("*") },
                            RequestBody.fromBytes(content)
                        )
                        true
                    } catch (exception: S3Exception) {
                        assertEquals(412, exception.statusCode())
                        false
                    }
                }
            }
            val successes = try {
                start.countDown()
                outcomes.count { it.get(1, TimeUnit.MINUTES) }
            } finally {
                executor.shutdownNow()
            }

            assertEquals(1, successes)
            assertEquals(content.size.toLong(), client.headObject { it.bucket(BUCKET).key(KEY) }.contentLength())

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

    @Test
    fun `Requests which cannot be served are answered with an S3 error`() {
        createClient().use { client ->
            createBucket(client, BUCKET)

            assertEquals(400, sendUnsigned("GET", "/Invalid_Bucket_Name"))
            assertS3Error("KeyTooLongError") { putObjectWithContent(client, BUCKET, "k".repeat(300), "Test") }
            assertS3Error("NotImplemented") { client.getBucketVersioning { it.bucket(BUCKET) } }

            val exception = assertThrows<S3Exception> { client.headObject { it.bucket(BUCKET).key("missing") } }
            assertNotNull(exception.requestId())

            assertEquals(204, client.deleteBucket { it.bucket(BUCKET) }.sdkHttpResponse().statusCode())
        }
    }

    @Test
    fun `Requests which are no valid upload do not overwrite the object`() {
        createClient().use { client ->
            client.createBucket { it.bucket(BUCKET).acl(BucketCannedACL.PUBLIC_READ_WRITE) }
            putObjectWithContent(client, BUCKET, KEY, "Test")

            assertEquals(405, sendUnsigned("POST", "/$BUCKET/$KEY", "Changed"))
            assertEquals(400, sendUnsigned("PUT", "/$BUCKET/$KEY?partNumber=1", "Changed"))
            assertEquals(400, sendUnsigned("PUT", "/$BUCKET/$KEY?partNumber=1&uploadId=", "Changed"))
            assertEquals("Test", client.getObject { it.bucket(BUCKET).key(KEY) }.use { String(it.readAllBytes()) })

            cleanupBuckets(client, BUCKET)
        }
    }

    @Test
    fun `Object keeps the headers which describe it and none of its upload request`() {
        createClient().use { client ->
            client.createBucket { it.bucket(BUCKET).acl(BucketCannedACL.PUBLIC_READ_WRITE) }
            val data = gzip("Test ".repeat(1000))

            client.putObject(
                {
                    it.bucket(BUCKET).key(KEY).contentType("text/plain").contentEncoding("gzip").contentLanguage("de")
                        .metadata(mapOf("color" to "blue")).contentMD5(base64(md5(data)))
                },
                RequestBody.fromBytes(data)
            )

            val head = client.headObject { it.bucket(BUCKET).key(KEY) }
            assertEquals("gzip", head.contentEncoding())
            assertEquals("de", head.contentLanguage())
            assertEquals(mapOf("color" to "blue"), head.metadata())
            listOf("x-amz-date", "x-amz-content-sha256", "x-amz-decoded-content-length", "Content-MD5").forEach {
                assertTrue(head.sdkHttpResponse().matchingHeaders(it).isEmpty(), "$it must not be returned")
            }

            val connection = URI("${BaseSdkSupportTest.ENDPOINT}/$BUCKET/$KEY").toURL().openConnection()
            connection.setRequestProperty("Accept-Encoding", "gzip")
            assertContentEquals(data, connection.getInputStream().use { it.readAllBytes() })
            assertEquals("gzip", connection.getHeaderField("Content-Encoding"))

            putObjectWithContent(client, BUCKET, "plain", "Test")
            assertNull(client.headObject { it.bucket(BUCKET).key("plain") }.contentEncoding())

            cleanupBuckets(client, BUCKET)
        }
    }

    @Test
    fun `Checksum of an object is not sent along with a range of it`() {
        createClient().use { client ->
            createBucket(client, BUCKET)
            val data = "Test".toByteArray()
            client.putObject(
                { it.bucket(BUCKET).key(KEY).checksumSHA256(base64(sha256(data))) }, RequestBody.fromBytes(data)
            )

            val whole = client.getObject { it.bucket(BUCKET).key(KEY).checksumMode(ChecksumMode.ENABLED) }
            assertEquals(base64(sha256(data)), whole.use { it.response() }.checksumSHA256())

            val range = client.getObject {
                it.bucket(BUCKET).key(KEY).range("bytes=0-1").checksumMode(ChecksumMode.ENABLED)
            }
            assertNull(range.response().checksumSHA256())
            assertEquals("Te", range.use { String(it.readAllBytes()) })

            cleanupBuckets(client, BUCKET)
        }
    }

    @Test
    fun `CopyObject honours the metadata directive`() {
        createClient().use { client ->
            createBucket(client, BUCKET)
            client.putObject(
                { it.bucket(BUCKET).key(KEY).contentType("text/plain").metadata(mapOf("color" to "blue")) },
                RequestBody.fromBytes("Test".toByteArray())
            )

            client.copyObject {
                it.sourceBucket(BUCKET).sourceKey(KEY).destinationBucket(BUCKET).destinationKey("kept")
            }
            val kept = client.headObject { it.bucket(BUCKET).key("kept") }
            assertEquals("text/plain", kept.contentType())
            assertEquals(mapOf("color" to "blue"), kept.metadata())

            client.copyObject {
                it.sourceBucket(BUCKET).sourceKey(KEY).destinationBucket(BUCKET).destinationKey("replaced")
                    .metadataDirective(MetadataDirective.REPLACE).contentType("application/json")
                    .metadata(mapOf("shape" to "round"))
            }
            val replaced = client.headObject { it.bucket(BUCKET).key("replaced") }
            assertEquals("application/json", replaced.contentType())
            assertEquals(mapOf("shape" to "round"), replaced.metadata())

            assertS3Error("InvalidRequest") {
                client.copyObject {
                    it.sourceBucket(BUCKET).sourceKey(KEY).destinationBucket(BUCKET).destinationKey(KEY)
                }
            }
            client.copyObject {
                it.sourceBucket(BUCKET).sourceKey(KEY).destinationBucket(BUCKET).destinationKey(KEY)
                    .metadataDirective(MetadataDirective.REPLACE).metadata(mapOf("shape" to "round"))
            }
            assertEquals(mapOf("shape" to "round"), client.headObject { it.bucket(BUCKET).key(KEY) }.metadata())

            assertS3Error("InvalidArgument") {
                client.copyObject {
                    it.sourceBucket(BUCKET).sourceKey(KEY).destinationBucket(BUCKET).destinationKey("copy")
                        .metadataDirective("REPLAC")
                }
            }

            cleanupBuckets(client, BUCKET)
        }
    }

    @Test
    fun `Bucket queries answer with the documents of S3`() {
        createClient().use { client ->
            createBucket(client, BUCKET)

            assertEquals("EU", client.getBucketLocation { it.bucket(BUCKET) }.locationConstraintAsString())

            val acl = client.getBucketAcl { it.bucket(BUCKET) }
            assertNotNull(acl.owner().id())
            assertEquals(listOf(Permission.FULL_CONTROL), acl.grants().map { it.permission() })
            client.putBucketAcl { it.bucket(BUCKET).acl(BucketCannedACL.PRIVATE) }

            assertS3Error("NoSuchCORSConfiguration") { client.getBucketCors { it.bucket(BUCKET) } }
            client.putBucketCors {
                it.bucket(BUCKET).corsConfiguration { cors ->
                    cors.corsRules(CORSRule.builder().allowedMethods("GET").allowedOrigins("*").build())
                }
            }
            client.deleteBucketCors { it.bucket(BUCKET) }
            assertEquals(Payer.BUCKET_OWNER, client.getBucketRequestPayment { it.bucket(BUCKET) }.payer())

            cleanupBuckets(client, BUCKET)
        }
    }

    @Test
    fun `DeleteObjects reports keys which do not exist as deleted`() {
        createClient().use { client ->
            createBucket(client, BUCKET)
            putObjectWithContent(client, BUCKET, KEY, "Test")

            val result = client.deleteObjects { request ->
                request.bucket(BUCKET).delete { delete ->
                    delete.objects(
                        ObjectIdentifier.builder().key(KEY).build(),
                        ObjectIdentifier.builder().key("missing").build()
                    )
                }
            }

            assertEquals(listOf(KEY, "missing"), result.deleted().map { it.key() })
            assertTrue(result.errors().isEmpty())
            assertTrue(client.listObjectsV2 { it.bucket(BUCKET) }.contents().isEmpty())

            cleanupBuckets(client, BUCKET)
        }
    }

    @Test
    fun `GetObject answers InvalidRange for a range behind the end of the object`() {
        createClient().use { client ->
            createBucket(client, BUCKET)
            putObjectWithContent(client, BUCKET, KEY, "Test")

            assertS3Error("InvalidRange") { client.getObject { it.bucket(BUCKET).key(KEY).range("bytes=4-10") } }
            val lastByte = client.getObject { it.bucket(BUCKET).key(KEY).range("bytes=3-10") }
            assertEquals("t", lastByte.use { String(it.readAllBytes()) })

            val suffix = client.getObject { it.bucket(BUCKET).key(KEY).range("bytes=-2") }
            assertEquals("st", suffix.use { String(it.readAllBytes()) })
            val suffixBeyondStart = client.getObject { it.bucket(BUCKET).key(KEY).range("bytes=-10") }
            assertEquals("bytes 0-3/4", suffixBeyondStart.response().contentRange())
            assertEquals("Test", suffixBeyondStart.use { String(it.readAllBytes()) })
            assertS3Error("InvalidRange") { client.getObject { it.bucket(BUCKET).key(KEY).range("bytes=-0") } }

            putObjectWithContent(client, BUCKET, "empty", "")
            assertS3Error("InvalidRange") { client.getObject { it.bucket(BUCKET).key("empty").range("bytes=-1") } }

            cleanupBuckets(client, BUCKET)
        }
    }

    @Test
    fun `Objects are listed in the binary order of their UTF-8 keys`() {
        createClient().use { client ->
            createBucket(client, BUCKET)
            listOf("ü", "a", "Z", "😇", "a/1").forEach { putObjectWithContent(client, BUCKET, it, "Test") }

            val expectedKeys = listOf("Z", "a", "a/1", "ü", "😇")
            assertEquals(expectedKeys, client.listObjectsV2 { it.bucket(BUCKET) }.contents().map { it.key() })
            assertEquals(expectedKeys, client.listObjects { it.bucket(BUCKET) }.contents().map { it.key() })

            cleanupBuckets(client, BUCKET)
        }
    }

    @Test
    fun `Listing parameters without a value are not taken for a sub-resource`() {
        createClient().use { client ->
            client.createBucket { it.bucket(BUCKET).acl(BucketCannedACL.PUBLIC_READ_WRITE) }

            assertEquals(200, sendUnsigned("GET", "/$BUCKET?prefix=&max-keys=10"))
            assertEquals(200, sendUnsigned("GET", "/$BUCKET?delimiter=&list-type=2&prefix="))
            assertEquals(200, sendUnsigned("GET", "/$BUCKET?encoding-type=&marker="))
            assertEquals(501, sendUnsigned("GET", "/$BUCKET?versioning"))

            cleanupBuckets(client, BUCKET)
        }
    }

    @Test
    fun `Objects are rolled up into common prefixes by a delimiter`() {
        createClient().use { client ->
            createBucket(client, BUCKET)
            listOf("a/1", "a/2", "b/x/1", "c").forEach { putObjectWithContent(client, BUCKET, it, "Test") }

            val root = client.listObjectsV2 { it.bucket(BUCKET).delimiter("/") }
            assertEquals(listOf("c"), root.contents().map { it.key() })
            assertEquals(listOf("a/", "b/"), root.commonPrefixes().map { it.prefix() })
            assertEquals(3, root.keyCount())

            val folder = client.listObjectsV2 { it.bucket(BUCKET).delimiter("/").prefix("a/") }
            assertEquals(listOf("a/1", "a/2"), folder.contents().map { it.key() })
            assertTrue(folder.commonPrefixes().isEmpty())

            val pagesV2 = client.listObjectsV2Paginator { it.bucket(BUCKET).delimiter("/").maxKeys(1) }.toList()
            assertEquals(3, pagesV2.size)
            assertEquals(listOf("a/", "b/"), pagesV2.flatMap { it.commonPrefixes() }.map { it.prefix() })
            assertEquals(listOf("c"), pagesV2.flatMap { it.contents() }.map { it.key() })

            val firstPageV1 = client.listObjects { it.bucket(BUCKET).delimiter("/").maxKeys(1) }
            assertEquals(listOf("a/"), firstPageV1.commonPrefixes().map { it.prefix() })
            assertTrue(firstPageV1.isTruncated())
            assertEquals("a/", firstPageV1.nextMarker())
            val secondPageV1 = client.listObjects {
                it.bucket(BUCKET).delimiter("/").maxKeys(1).marker(firstPageV1.nextMarker())
            }
            assertEquals(listOf("b/"), secondPageV1.commonPrefixes().map { it.prefix() })

            cleanupBuckets(client, BUCKET)
        }
    }

    @Test
    fun `Listing continues after a key which does not have to exist`() {
        createClient().use { client ->
            createBucket(client, BUCKET)
            listOf("a/1", "a/2", "b/1", "sp ace").forEach { putObjectWithContent(client, BUCKET, it, "Test") }

            val afterMissingKey = client.listObjectsV2 { it.bucket(BUCKET).startAfter("a/1x") }
            assertEquals(listOf("a/2", "b/1", "sp ace"), afterMissingKey.contents().map { it.key() })
            val afterMissingMarker = client.listObjects { it.bucket(BUCKET).marker("a/1x") }
            assertEquals(listOf("a/2", "b/1", "sp ace"), afterMissingMarker.contents().map { it.key() })

            assertS3Error("InvalidArgument") {
                client.listObjectsV2 { it.bucket(BUCKET).continuationToken("not a token!") }
            }

            assertEquals(1000, client.listObjectsV2 { it.bucket(BUCKET).maxKeys(5000) }.maxKeys())
            val nothing = client.listObjectsV2 { it.bucket(BUCKET).maxKeys(0) }
            assertTrue(nothing.contents().isEmpty())
            assertFalse(nothing.isTruncated())

            val encoded = client.listObjectsV2 { it.bucket(BUCKET).prefix("sp").encodingType(EncodingType.URL) }
            assertEquals(EncodingType.URL, encoded.encodingType())
            assertEquals(
                listOf("sp ace"), encoded.contents().map { URLDecoder.decode(it.key(), StandardCharsets.UTF_8) }
            )

            cleanupBuckets(client, BUCKET)
        }
    }

    @Test
    fun `A policy parameter does not exempt the POST requests of the API from being signed`() {
        createClient().use { client ->
            createBucket(client, BUCKET)
            putObjectWithContent(client, BUCKET, KEY, "Test")
            val uploadId = client.createMultipartUpload { it.bucket(BUCKET).key("upload") }.uploadId()

            assertEquals(403, sendUnsigned("POST", "/$BUCKET/other?uploads&policy=x"))
            assertEquals(
                403,
                sendUnsigned("POST", "/$BUCKET/upload?uploadId=$uploadId&Signature=x", "<CompleteMultipartUpload/>")
            )
            assertEquals(
                403,
                sendUnsigned("POST", "/$BUCKET?delete&policy=x", "<Delete><Object><Key>$KEY</Key></Object></Delete>")
            )

            assertEquals(listOf(KEY), client.listObjectsV2 { it.bucket(BUCKET) }.contents().map { it.key() })
            assertEquals(listOf("upload"), client.listMultipartUploads { it.bucket(BUCKET) }.uploads().map { it.key() })

            cleanupBuckets(client, BUCKET)
        }
    }

    @Test
    fun `A bucket created anew is private, even if a public one of that name was deleted`() {
        createClient().use { client ->
            client.createBucket { it.bucket(BUCKET).acl(BucketCannedACL.PUBLIC_READ_WRITE) }
            putObjectWithContent(client, BUCKET, KEY, "Test")
            assertEquals(200, sendUnsigned("GET", "/$BUCKET/$KEY"))
            cleanupBuckets(client, BUCKET)

            createBucket(client, BUCKET)
            putObjectWithContent(client, BUCKET, KEY, "Test")
            assertEquals(403, sendUnsigned("GET", "/$BUCKET/$KEY"))

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

    private fun gzip(text: String): ByteArray {
        val buffer = ByteArrayOutputStream()
        GZIPOutputStream(buffer).use { it.write(text.toByteArray()) }
        return buffer.toByteArray()
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
