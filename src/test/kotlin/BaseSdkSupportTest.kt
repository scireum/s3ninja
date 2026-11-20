/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

import com.google.common.io.BaseEncoding
import com.google.common.io.ByteStreams
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import sirius.kernel.SiriusExtension
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.services.s3.model.*
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest
import java.io.File
import java.io.FileWriter
import java.net.HttpURLConnection
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.security.MessageDigest
import java.time.Duration
import java.time.Instant
import java.util.Base64
import kotlin.test.*

@ExtendWith(SiriusExtension::class)
abstract class BaseSdkSupportTest {

    abstract fun getClient(): S3Client
    abstract fun getAsyncClient(): S3AsyncClient

    /**
     * Before each test, delete all buckets and their objects. This allows running the test based on a clean state.
     */
    @BeforeEach
    fun setup() {
        getClient().use { client ->
            client.listBuckets().buckets()?.forEach { bucket ->
                client.listObjects { it.bucket(bucket.name()) }.contents()?.forEach { obj ->
                    deleteObject(client, bucket.name(), obj.key())
                }
                deleteBucket(client, bucket.name())
            }
        }
    }

    @Test
    fun `HEAD of non-existing bucket as expected`() {
        val bucketName = "does-not-exist"

        getClient().use { client ->
            assertFalse(doesBucketExist(client, bucketName))
        }
    }

    @Test
    fun `PUT and then HEAD bucket as expected`() {
        val bucketName = DEFAULT_BUCKET_NAME

        getClient().use { client ->
            assertFalse(doesBucketExist(client, bucketName))
            createBucket(client, bucketName)
            assertTrue(doesBucketExist(client, bucketName))
            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `DELETE of non-existing bucket as expected`() {
        val bucketName = "does-not-exist"

        getClient().use { client ->
            assertFalse(doesBucketExist(client, bucketName))
            assertThrows<S3Exception> {
                deleteBucket(client, bucketName)
            }
            assertFalse(doesBucketExist(client, bucketName))
        }
    }

    @Test
    fun `PUT and then DELETE bucket as expected`() {
        val bucketName = DEFAULT_BUCKET_NAME

        getClient().use { client ->
            createBucket(client, bucketName)
            assertTrue(doesBucketExist(client, bucketName))

            cleanupBuckets(client, bucketName)
            assertFalse(doesBucketExist(client, bucketName))
        }
    }

    @Test
    fun `PUT and then GET file work using TransferManager`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getAsyncClient().use { client ->
            createBucket(client, bucketName)

            val file = File.createTempFile("test", "")
            file.deleteOnExit()

            FileWriter(file, StandardCharsets.UTF_8, true).use { writer ->
                for (ignored in 0..10000) {
                    writer.write("$ignored. This is a test.\n")
                }
            }

            S3TransferManager.builder().s3Client(client).build().use { transferManager ->
                val uploadFileRequest = UploadFileRequest.builder().putObjectRequest { it.bucket(bucketName).key(key) }
                    .source(Paths.get(file.toPath().toUri())).build()
                transferManager.uploadFile(uploadFileRequest).completionFuture().join()

                val download = File.createTempFile("s3-test", "")
                download.deleteOnExit()

                val downloadFileRequest =
                    DownloadFileRequest.builder().getObjectRequest { it.bucket(bucketName).key(key) }
                        .destination(download)
                        .build()
                transferManager.downloadFile(downloadFileRequest).completionFuture().join()

                assertEquals(file.readText(), download.readText())

                cleanupBuckets(client, bucketName)
            }
        }
    }

    @Test
    fun `PUT and then GET work as expected`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getClient().use { client ->
            createBucket(client, bucketName)

            putObjectWithContent(client, bucketName, key, "Test")

            val getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build()

            val content = client.getObject(getObjectRequest) { _, inputStream ->
                String(ByteStreams.toByteArray(inputStream), StandardCharsets.UTF_8)
            }

            getPresigner().use { presigner ->
                val presignedRequest = presigner.presignGetObject { builder ->
                    builder.getObjectRequest { it.bucket(bucketName).key(key) }
                        .signatureDuration(Duration.ofMinutes(10))
                }
                val connection = URI(presignedRequest.url().toString()).toURL().openConnection()
                val downloadedData =
                    String(ByteStreams.toByteArray(connection.getInputStream()), StandardCharsets.UTF_8)

                assertEquals("Test", content)
                assertEquals("Test", downloadedData)
            }

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `PUT and then LIST work as expected`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key1 = "$DEFAULT_KEY/Eins"
        val key2 = "$DEFAULT_KEY/Zwei"

        getClient().use { client ->
            createBucket(client, bucketName)

            putObjectWithContent(client, bucketName, key1, "Eins")
            putObjectWithContent(client, bucketName, key2, "Zwei")

            val listing = client.listObjects { it.bucket(bucketName) }
            val summaries = listing.contents()

            assertEquals(2, summaries.size)
            assertEquals(key1, summaries.first().key())
            assertEquals(key2, summaries[1].key())

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `PUT and then DELETE work as expected`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getClient().use { client ->
            createBucket(client, bucketName)

            assertThrows<S3Exception> {
                putObjectWithContent(client, bucketName, key, "Test")
                deleteObject(client, bucketName, key)
                getObject(client, bucketName, key)
            }

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `MultipartUpload and then GET work as expected`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getAsyncClient().use { client ->
            val message = "Test".toByteArray(StandardCharsets.UTF_8)
            val tempFile = File.createTempFile("upload", null).apply {
                writeBytes(message)
            }
            tempFile.deleteOnExit()

            createBucket(client, bucketName)

            val putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .metadata(mapOf("userdata" to "test123"))
                .contentLength(message.size.toLong())
                .build()

            val uploadFileRequest = UploadFileRequest.builder()
                .putObjectRequest(putObjectRequest)
                .source(tempFile.toPath())
                .build()

            S3TransferManager.builder()
                .s3Client(client)
                .build().use { transferManager ->
                    transferManager.uploadFile(uploadFileRequest).completionFuture().join()
                }

            val getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build()

            val contentFuture = client.getObject(getObjectRequest, AsyncResponseTransformer.toBytes())
            val content = String(contentFuture.join().asByteArray(), StandardCharsets.UTF_8)

            assertEquals("Test", content)

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `MultipartUpload and then DELETE work as expected`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getClient().use { client ->
            val message = "Test".toByteArray(StandardCharsets.UTF_8)

            createBucket(client, bucketName)

            val createMultipartUploadResponse = client.createMultipartUpload {
                it.bucket(bucketName).key(key)
            }

            val uploadId = createMultipartUploadResponse.uploadId()
            val partNumber = 1

            val uploadPartResponse = client.uploadPart(
                UploadPartRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId)
                    .partNumber(partNumber)
                    .contentLength(message.size.toLong())
                    .build(),
                RequestBody.fromBytes(message)
            )
            val completedPart = CompletedPart.builder()
                .partNumber(partNumber)
                .eTag(uploadPartResponse.eTag())
                .build()

            client.completeMultipartUpload {
                it.bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId)
                    .multipartUpload(
                        CompletedMultipartUpload.builder()
                            .parts(completedPart)
                            .build()
                    )
            }

            assertThrows<S3Exception> {
                deleteObject(client, bucketName, key)
                getObject(client, bucketName, key)
            }

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `PUT on presigned URL without signed chunks works as expected`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY
        val content = "NotSigned"

        getClient().use { client ->
            createBucket(client, bucketName)

            getPresigner().use { presigner ->
                val putPresignedRequest = presigner.presignPutObject { builder ->
                    builder.putObjectRequest { it.bucket(bucketName).key(key) }
                        .signatureDuration(Duration.ofMinutes(10))
                }

                val putUrl = URI(putPresignedRequest.url().toString()).toURL()
                val putConnection = putUrl.openConnection() as HttpURLConnection
                putConnection.doOutput = true
                putConnection.requestMethod = "PUT"
                putConnection.outputStream.use { it.write(content.toByteArray(StandardCharsets.UTF_8)) }

                assertEquals(200, putConnection.responseCode)

                val getPresignedRequest = presigner.presignGetObject { builder ->
                    builder.getObjectRequest { it.bucket(bucketName).key(key) }
                        .signatureDuration(Duration.ofMinutes(10))
                }

                val getUrl = URI(getPresignedRequest.url().toString()).toURL()
                val getConnection = getUrl.openConnection()
                val downloadedData =
                    String(ByteStreams.toByteArray(getConnection.getInputStream()), StandardCharsets.UTF_8)

                assertEquals(content, downloadedData)
            }

            cleanupBuckets(client, bucketName)
        }
    }

    // reported in https://github.com/scireum/s3ninja/issues/153
    @Test
    fun `PUT and then GET on presigned URL with ResponseHeaderOverrides works as expected`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getClient().use { client ->
            createBucket(client, bucketName)

            putObjectWithContent(client, bucketName, key, "Test")
            val content = client.getObject { builder ->
                builder.bucket(bucketName).key(key)
            }.use { inputStream ->
                String(ByteStreams.toByteArray(inputStream), StandardCharsets.UTF_8)
            }

            assertEquals("Test", content)

            getPresigner().use { presigner ->
                val presignedRequest = presigner.presignGetObject { builder ->
                    builder.getObjectRequest { it.bucket(bucketName).key(key) }
                        .signatureDuration(Duration.ofHours(1))
                        .getObjectRequest {
                            it.bucket(bucketName).key(key).responseContentDisposition("inline; filename=\"hello.txt\"")
                        }
                }

                val connection = URI(presignedRequest.url().toString()).toURL().openConnection()
                val downloadedData = connection.getInputStream().use { inputStream ->
                    String(ByteStreams.toByteArray(inputStream), StandardCharsets.UTF_8)
                }

                assertEquals("Test", downloadedData)
            }

            cleanupBuckets(client, bucketName)
        }
    }

    // reported in https://github.com/scireum/s3ninja/issues/181
    @Test
    fun `Bulk delete using DeleteObjectCommand works as expected`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key1 = "$DEFAULT_KEY/Eins"
        val key2 = "$DEFAULT_KEY/Zwei"
        val key3 = "$DEFAULT_KEY/Drei"

        getClient().use { client ->
            createBucket(client, bucketName)

            putObjectWithContent(client, bucketName, key1, "Eins")
            putObjectWithContent(client, bucketName, key2, "Zwei")
            putObjectWithContent(client, bucketName, key3, "Drei")
            val deleteObjectsRequest = DeleteObjectsRequest.builder()
                .bucket(bucketName)
                .delete(
                    Delete.builder()
                        .objects(
                            ObjectIdentifier.builder().key(key1).build(),
                            ObjectIdentifier.builder().key(key2).build()
                        )
                        .build()
                )
                .build()

            val result = client.deleteObjects(deleteObjectsRequest)

            assertEquals(2, result.deleted().size)
            assertEquals(key1, result.deleted()[0].key())
            assertEquals(key2, result.deleted()[1].key())

            val listing = client.listObjects { it.bucket(bucketName) }
            assertEquals(1, listing.contents().size)
            assertEquals(key3, listing.contents()[0].key())

            cleanupBuckets(client, bucketName)
        }
    }

    // reported in https://github.com/scireum/s3ninja/issues/214
    @Test
    fun `ListObjectsV2 supports prefix as expected`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key1 = "$DEFAULT_KEY/Eins"
        val key2 = "$DEFAULT_KEY/Eins-Eins"
        val key3 = "$DEFAULT_KEY/Drei"

        getClient().use { client ->
            createBucket(client, bucketName)

            putObjectWithContent(client, bucketName, key1, "Eins")
            putObjectWithContent(client, bucketName, key2, "Zwei")
            putObjectWithContent(client, bucketName, key3, "Drei")

            val listObjectsV2Request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(key1)
                .build()

            val result = client.listObjectsV2(listObjectsV2Request)

            assertEquals(2, result.keyCount())
            assertEquals(2, result.contents().size)
            assertEquals(key1, result.contents()[0].key())
            assertEquals(key2, result.contents()[1].key())

            cleanupBuckets(client, bucketName)
        }
    }

    // reported in https://github.com/scireum/s3ninja/issues/209
    @Test
    fun `HEAD reports content length correctly`() {
        val bucketName = "public-bucket"
        val key = "simple_test"
        val content = "I am pointless text content"

        getClient().use { client ->
            client.createBucket {
                it.bucket(bucketName)
                    .acl(BucketCannedACL.PUBLIC_READ_WRITE)
            }

            putObjectWithContent(client, bucketName, key, content)

            val url = URI("http://localhost:9999/$bucketName/$key").toURL()
            val connection = (url.openConnection() as HttpURLConnection).apply {
                requestMethod = "HEAD"
            }

            assertEquals(200, connection.responseCode)
            assertEquals(content.toByteArray(StandardCharsets.UTF_8).size.toLong(), connection.contentLengthLong)

            connection.disconnect()

            cleanupBuckets(client, bucketName)
        }
    }

    // reported in https://github.com/scireum/s3ninja/issues/230
    @Test
    fun `Copying an object within the same bucket works as expected`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val keyFrom = DEFAULT_KEY
        val keyTo = "$keyFrom-copy"
        val content = "I am pointless text content, but I deserve to exist twice and will thus be copied!"

        getClient().use { client ->
            createBucket(client, bucketName)

            putObjectWithContent(client, bucketName, keyFrom, content)

            client.copyObject {
                it.sourceBucket(bucketName)
                    .sourceKey(keyFrom)
                    .destinationBucket(bucketName)
                    .destinationKey(keyTo)
            }

            getPresigner().use { presigner ->
                val presignedRequest = presigner.presignGetObject { builder ->
                    builder.getObjectRequest { it.bucket(bucketName).key(keyTo) }
                        .signatureDuration(Duration.ofMinutes(10))
                }

                val connection = URI(presignedRequest.url().toString()).toURL().openConnection()
                val downloadedData = connection.getInputStream().use { inputStream ->
                    String(ByteStreams.toByteArray(inputStream), StandardCharsets.UTF_8)
                }

                assertEquals(content, downloadedData)
            }

            cleanupBuckets(client, bucketName)
        }
    }

    // reported in https://github.com/scireum/s3ninja/issues/230
    @Test
    fun `Copying an object across buckets works as expected`() {
        val bucketNameFrom = DEFAULT_BUCKET_NAME
        val bucketNameTo = "$DEFAULT_BUCKET_NAME-copy"
        val key = DEFAULT_KEY
        val content = "I am pointless text content, but I deserve to exist twice and will thus be copied!"

        getClient().use { client ->
            createBucket(client, bucketNameFrom)
            createBucket(client, bucketNameTo)

            putObjectWithContent(client, bucketNameFrom, key, content)

            client.copyObject {
                it.sourceBucket(bucketNameFrom)
                    .sourceKey(key)
                    .destinationBucket(bucketNameTo)
                    .destinationKey(key)
            }

            getPresigner().use { presigner ->
                val presignedRequest = presigner.presignGetObject { builder ->
                    builder.getObjectRequest { it.bucket(bucketNameTo).key(key) }
                        .signatureDuration(Duration.ofMinutes(10))
                }

                val connection = URI(presignedRequest.url().toString()).toURL().openConnection()
                val downloadedData = connection.getInputStream().use { inputStream ->
                    String(ByteStreams.toByteArray(inputStream), StandardCharsets.UTF_8)
                }

                assertEquals(content, downloadedData)
            }

            cleanupBuckets(client, bucketNameFrom, bucketNameTo)
        }
    }

    // reported in https://github.com/scireum/s3ninja/issues/251
    @Test
    fun `ListObjectsV2 supports pagination as expected`() {
        val maxKeys = 2
        val bucketName = DEFAULT_BUCKET_NAME
        val key1 = "$DEFAULT_KEY/Eins"
        val key2 = "$DEFAULT_KEY/Eins-Eins"
        val key3 = "$DEFAULT_KEY/Drei"

        getClient().use { client ->
            createBucket(client, bucketName)

            putObjectWithContent(client, bucketName, key1, "Eins")
            putObjectWithContent(client, bucketName, key2, "Zwei")
            putObjectWithContent(client, bucketName, key3, "Drei")

            val request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .maxKeys(maxKeys)
                .build()

            val result = client.listObjectsV2(request)

            assertEquals(maxKeys, result.keyCount(), "keyCount must match the requested maxKeys")
            assertEquals(maxKeys, result.maxKeys(), "maxKeys must match the request")
            assertEquals(maxKeys, result.contents().size)
            assertEquals(key3, result.contents()[0].key())
            assertEquals(key1, result.contents()[1].key())
            assertNotNull(result.nextContinuationToken())

            val followupRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .maxKeys(maxKeys)
                .continuationToken(result.nextContinuationToken())
                .build()

            val followupResult = client.listObjectsV2(followupRequest)

            assertEquals(3 - maxKeys, followupResult.keyCount(), "keyCount must match the requested maxKeys")
            assertEquals(maxKeys, followupResult.maxKeys(), "maxKeys must match the request")
            assertEquals(3 - maxKeys, followupResult.contents().size)
            assertEquals(key2, followupResult.contents()[0].key())
            assertEquals(followupRequest.continuationToken(), followupResult.continuationToken())
            assertNull(followupResult.nextContinuationToken())

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `Multipart upload IDs are opaque and never handed out twice`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getClient().use { client ->
            createBucket(client, bucketName)

            val firstUploadId = startMultipartUpload(client, bucketName, key)
            val secondUploadId = startMultipartUpload(client, bucketName, key)

            assertNotEquals(firstUploadId, secondUploadId)
            assertNull(firstUploadId.toLongOrNull(), "upload IDs must not be drawn from a counter")
            assertNull(secondUploadId.toLongOrNull(), "upload IDs must not be drawn from a counter")
            assertTrue(firstUploadId.length >= 32, "upload IDs must not be guessable")

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `Multipart upload is bound to its bucket and key`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getClient().use { client ->
            createBucket(client, bucketName)

            val uploadId = startMultipartUpload(client, bucketName, key)

            assertS3Error("NoSuchUpload") {
                uploadPart(client, bucketName, "$key-other", uploadId, 1, "Test".toByteArray())
            }
            uploadPart(client, bucketName, key, uploadId, 1, "Test".toByteArray())

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `CompleteMultipartUpload naming a missing part answers InvalidPart and keeps the upload open`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getClient().use { client ->
            createBucket(client, bucketName)

            val uploadId = startMultipartUpload(client, bucketName, key)
            val firstPart = uploadPart(client, bucketName, key, uploadId, 1, "Test".toByteArray())
            val missingPart = CompletedPart.builder().partNumber(2).eTag(firstPart.eTag()).build()

            assertS3Error("InvalidPart") {
                completeMultipartUpload(client, bucketName, key, uploadId, firstPart, missingPart)
            }

            completeMultipartUpload(client, bucketName, key, uploadId, firstPart)
            assertEquals("Test", getObject(client, bucketName, key)!!.use { String(it.readAllBytes()) })

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `CompleteMultipartUpload with a wrong ETag answers InvalidPart and keeps the upload open`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getClient().use { client ->
            createBucket(client, bucketName)

            val uploadId = startMultipartUpload(client, bucketName, key)
            val part = uploadPart(client, bucketName, key, uploadId, 1, "Test".toByteArray())
            val forgedPart = CompletedPart.builder().partNumber(1).eTag("deadbeef").build()

            assertS3Error("InvalidPart") {
                completeMultipartUpload(client, bucketName, key, uploadId, forgedPart)
            }
            assertS3Error("NoSuchKey") { getObject(client, bucketName, key) }

            completeMultipartUpload(client, bucketName, key, uploadId, part)
            assertEquals("Test", getObject(client, bucketName, key)!!.use { String(it.readAllBytes()) })

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `CompleteMultipartUpload accepts ETags with and without quotes`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getClient().use { client ->
            createBucket(client, bucketName)

            listOf<(String) -> String>({ "\"$it\"" }, { it }).forEach { quoting ->
                val uploadId = startMultipartUpload(client, bucketName, key)
                val part = uploadPart(client, bucketName, key, uploadId, 1, "Test".toByteArray())
                val requotedPart =
                    CompletedPart.builder().partNumber(1).eTag(quoting(part.eTag().trim('"'))).build()

                completeMultipartUpload(client, bucketName, key, uploadId, requotedPart)
            }

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `CompleteMultipartUpload with parts out of order answers InvalidPartOrder`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getClient().use { client ->
            createBucket(client, bucketName)

            val uploadId = startMultipartUpload(client, bucketName, key)
            val firstPart = uploadPart(client, bucketName, key, uploadId, 1, bytesOfSize(MIN_PART_SIZE))
            val secondPart = uploadPart(client, bucketName, key, uploadId, 2, "Test".toByteArray())

            assertS3Error("InvalidPartOrder") {
                completeMultipartUpload(client, bucketName, key, uploadId, secondPart, firstPart)
            }
            assertS3Error("InvalidPartOrder") {
                completeMultipartUpload(client, bucketName, key, uploadId, firstPart, firstPart)
            }

            completeMultipartUpload(client, bucketName, key, uploadId, firstPart, secondPart)

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `CompleteMultipartUpload without any part or with an incomplete part answers MalformedXML`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getClient().use { client ->
            createBucket(client, bucketName)

            val uploadId = startMultipartUpload(client, bucketName, key)
            val part = uploadPart(client, bucketName, key, uploadId, 1, "Test".toByteArray())

            assertS3Error("MalformedXML") {
                completeMultipartUpload(client, bucketName, key, uploadId)
            }
            assertS3Error("MalformedXML") {
                completeMultipartUpload(
                    client, bucketName, key, uploadId,
                    CompletedPart.builder().eTag(part.eTag()).build()
                )
            }
            assertS3Error("MalformedXML") {
                completeMultipartUpload(
                    client, bucketName, key, uploadId,
                    CompletedPart.builder().partNumber(1).build()
                )
            }
            completeMultipartUpload(client, bucketName, key, uploadId, part)

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `CompleteMultipartUpload with an undersized part answers EntityTooSmall and keeps the upload open`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getClient().use { client ->
            createBucket(client, bucketName)

            val uploadId = startMultipartUpload(client, bucketName, key)
            val undersizedPart = uploadPart(client, bucketName, key, uploadId, 1, bytesOfSize(MIN_PART_SIZE - 1))
            val lastPart = uploadPart(client, bucketName, key, uploadId, 2, "Test".toByteArray())

            assertS3Error("EntityTooSmall") {
                completeMultipartUpload(client, bucketName, key, uploadId, undersizedPart, lastPart)
            }

            val firstPart = uploadPart(client, bucketName, key, uploadId, 1, bytesOfSize(MIN_PART_SIZE))
            completeMultipartUpload(client, bucketName, key, uploadId, firstPart, lastPart)

            val content = getObject(client, bucketName, key)!!.use { it.readAllBytes() }
            assertContentEquals(bytesOfSize(MIN_PART_SIZE) + "Test".toByteArray(), content)

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `HEAD and GET report the ETag and the Content-Type exactly once`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY
        val data = "Test".toByteArray()

        getClient().use { client ->
            createBucket(client, bucketName)

            val putResponse = client.putObject(
                PutObjectRequest.builder().bucket(bucketName).key(key).contentType("application/pdf").build(),
                RequestBody.fromBytes(data)
            )

            val headResponse = client.headObject { it.bucket(bucketName).key(key) }
            val getResponse = client.getObject { it.bucket(bucketName).key(key) }.use { it.response() }

            listOf(headResponse.sdkHttpResponse(), getResponse.sdkHttpResponse()).forEach { httpResponse ->
                assertEquals(listOf("application/pdf"), httpResponse.matchingHeaders("Content-Type"))
                assertEquals(listOf(putResponse.eTag()), httpResponse.matchingHeaders("ETag"))
            }
            assertTrue(putResponse.eTag().startsWith('"'), "ETags are quoted")

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `UploadPart and ListParts report the quoted MD5 of the part as ETag`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY
        val data = "Test".toByteArray()

        getClient().use { client ->
            createBucket(client, bucketName)

            val uploadId = startMultipartUpload(client, bucketName, key)
            val part = uploadPart(client, bucketName, key, uploadId, 1, data)

            assertEquals("\"${md5Hex(data)}\"", part.eTag())

            val listedParts = client.listParts { it.bucket(bucketName).key(key).uploadId(uploadId) }.parts()
            assertEquals(listOf("\"${md5Hex(data)}\""), listedParts.map { it.eTag() })

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `Multipart object carries the MD5 of the part MD5s and the number of parts as ETag`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY
        val firstData = bytesOfSize(MIN_PART_SIZE)
        val secondData = "Test".toByteArray()
        val expectedETag = "\"${md5Hex(md5(firstData) + md5(secondData))}-2\""

        getClient().use { client ->
            createBucket(client, bucketName)

            val uploadId = startMultipartUpload(client, bucketName, key)
            val firstPart = uploadPart(client, bucketName, key, uploadId, 1, firstData)
            val secondPart = uploadPart(client, bucketName, key, uploadId, 2, secondData)
            val response = completeMultipartUpload(client, bucketName, key, uploadId, firstPart, secondPart)

            assertEquals(expectedETag, response.eTag())
            assertEquals(expectedETag, client.headObject { it.bucket(bucketName).key(key) }.eTag())

            assertEquals(expectedETag, client.listObjectsV2 { it.bucket(bucketName) }.contents().single().eTag())
            assertEquals(expectedETag, client.listObjects { it.bucket(bucketName) }.contents().single().eTag())

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `Copy of a multipart object carries the MD5 of its content as ETag`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY
        val keyTo = "$key-copy"
        val data = "Test".toByteArray()

        getClient().use { client ->
            createBucket(client, bucketName)

            val uploadId = startMultipartUpload(client, bucketName, key)
            val part = uploadPart(client, bucketName, key, uploadId, 1, data)
            completeMultipartUpload(client, bucketName, key, uploadId, part)

            val response = client.copyObject {
                it.sourceBucket(bucketName).sourceKey(key).destinationBucket(bucketName).destinationKey(keyTo)
            }

            assertEquals("\"${md5Hex(data)}\"", response.copyObjectResult().eTag())
            assertEquals("\"${md5Hex(data)}\"", client.headObject { it.bucket(bucketName).key(keyTo) }.eTag())

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `UploadPart only accepts part numbers from 1 to 10000`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getClient().use { client ->
            createBucket(client, bucketName)

            val uploadId = startMultipartUpload(client, bucketName, key)

            assertS3Error("InvalidArgument") {
                uploadPart(client, bucketName, key, uploadId, 0, "Test".toByteArray())
            }
            assertS3Error("InvalidArgument") {
                uploadPart(client, bucketName, key, uploadId, 10001, "Test".toByteArray())
            }
            uploadPart(client, bucketName, key, uploadId, 10000, "Test".toByteArray())

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `AbortMultipartUpload answers 204 and NoSuchUpload for an unknown upload`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getClient().use { client ->
            createBucket(client, bucketName)

            val uploadId = startMultipartUpload(client, bucketName, key)
            uploadPart(client, bucketName, key, uploadId, 1, "Test".toByteArray())

            val response = client.abortMultipartUpload { it.bucket(bucketName).key(key).uploadId(uploadId) }
            assertEquals(204, response.sdkHttpResponse().statusCode())

            assertS3Error("NoSuchUpload") {
                uploadPart(client, bucketName, key, uploadId, 2, "Test".toByteArray())
            }
            assertS3Error("NoSuchUpload") {
                client.abortMultipartUpload { it.bucket(bucketName).key(key).uploadId(uploadId) }
            }

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `ListParts lists the uploaded parts in ascending order and supports pagination`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getClient().use { client ->
            createBucket(client, bucketName)

            val uploadId = startMultipartUpload(client, bucketName, key)
            listOf(3, 1, 10, 2).forEach { partNumber ->
                uploadPart(client, bucketName, key, uploadId, partNumber, bytesOfSize(partNumber))
            }

            val allParts = client.listParts { it.bucket(bucketName).key(key).uploadId(uploadId) }
            assertEquals(listOf(1, 2, 3, 10), allParts.parts().map { it.partNumber() })
            assertEquals(listOf(1L, 2L, 3L, 10L), allParts.parts().map { it.size() })
            assertFalse(allParts.isTruncated())

            val firstPage = client.listParts { it.bucket(bucketName).key(key).uploadId(uploadId).maxParts(3) }
            assertEquals(listOf(1, 2, 3), firstPage.parts().map { it.partNumber() })
            assertTrue(firstPage.isTruncated())
            assertEquals(3, firstPage.nextPartNumberMarker())

            val secondPage = client.listParts {
                it.bucket(bucketName).key(key).uploadId(uploadId).maxParts(3)
                    .partNumberMarker(firstPage.nextPartNumberMarker())
            }
            assertEquals(listOf(10), secondPage.parts().map { it.partNumber() })
            assertFalse(secondPage.isTruncated())

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `PutObject stores Content-Disposition, Cache-Control and Expires`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getClient().use { client ->
            createBucket(client, bucketName)

            client.putObject(
                PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentDisposition(CONTENT_DISPOSITION)
                    .cacheControl(CACHE_CONTROL)
                    .expires(EXPIRES)
                    .build(),
                RequestBody.fromBytes("Test".toByteArray())
            )

            assertCachingAndDispositionHeaders(client, bucketName, key)

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `Multipart upload stores Content-Disposition, Cache-Control and Expires`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getClient().use { client ->
            createBucket(client, bucketName)

            val uploadId = client.createMultipartUpload {
                it.bucket(bucketName)
                    .key(key)
                    .contentDisposition(CONTENT_DISPOSITION)
                    .cacheControl(CACHE_CONTROL)
                    .expires(EXPIRES)
            }.uploadId()
            val part = uploadPart(client, bucketName, key, uploadId, 1, "Test".toByteArray())
            completeMultipartUpload(client, bucketName, key, uploadId, part)

            assertCachingAndDispositionHeaders(client, bucketName, key)

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `ListMultipartUploads lists the open uploads of a bucket`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val otherBucketName = "$DEFAULT_BUCKET_NAME-other"

        getClient().use { client ->
            createBucket(client, bucketName)
            createBucket(client, otherBucketName)

            assertTrue(client.listMultipartUploads { it.bucket(bucketName) }.uploads().isEmpty())

            val uploadB = startMultipartUpload(client, bucketName, "b/1")
            val firstUploadA = startMultipartUpload(client, bucketName, "a/1")
            val secondUploadA = startMultipartUpload(client, bucketName, "a/1")
            val uploadA2 = startMultipartUpload(client, bucketName, "a/2 😇")
            startMultipartUpload(client, otherBucketName, "a/1")

            val allUploads = client.listMultipartUploads { it.bucket(bucketName) }
            assertEquals(listOf("a/1", "a/1", "a/2 😇", "b/1"), allUploads.uploads().map { it.key() })
            assertEquals(
                setOf(firstUploadA, secondUploadA, uploadA2, uploadB),
                allUploads.uploads().map { it.uploadId() }.toSet()
            )
            assertFalse(allUploads.isTruncated())
            assertNotNull(allUploads.uploads().first().initiated())

            val prefixedUploads = client.listMultipartUploads { it.bucket(bucketName).prefix("a/") }
            assertEquals(listOf("a/1", "a/1", "a/2 😇"), prefixedUploads.uploads().map { it.key() })

            val firstPage = client.listMultipartUploads { it.bucket(bucketName).maxUploads(1) }
            assertTrue(firstPage.isTruncated())
            val secondPage = client.listMultipartUploads {
                it.bucket(bucketName)
                    .keyMarker(firstPage.nextKeyMarker())
                    .uploadIdMarker(firstPage.nextUploadIdMarker())
            }
            assertEquals(
                allUploads.uploads().map { it.uploadId() },
                (firstPage.uploads() + secondPage.uploads()).map { it.uploadId() }
            )

            client.abortMultipartUpload { it.bucket(bucketName).key("b/1").uploadId(uploadB) }
            val part = uploadPart(client, bucketName, "a/1", firstUploadA, 1, "Test".toByteArray())
            completeMultipartUpload(client, bucketName, "a/1", firstUploadA, part)

            assertEquals(
                setOf(secondUploadA, uploadA2),
                client.listMultipartUploads { it.bucket(bucketName) }.uploads().map { it.uploadId() }.toSet()
            )

            cleanupBuckets(client, bucketName, otherBucketName)
        }
    }

    @Test
    fun `UploadPartCopy fills a part from an existing object instead of overwriting the target`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val sourceKey = "$DEFAULT_KEY-source"
        val key = DEFAULT_KEY
        val sourceData = bytesOfSize(MIN_PART_SIZE + 100)

        getClient().use { client ->
            createBucket(client, bucketName)
            client.putObject({ it.bucket(bucketName).key(sourceKey) }, RequestBody.fromBytes(sourceData))

            val uploadId = startMultipartUpload(client, bucketName, key)
            val firstPart = uploadPartCopy(client, bucketName, sourceKey, key, uploadId, 1, null)
            val secondPart = uploadPartCopy(client, bucketName, sourceKey, key, uploadId, 2, "bytes=10-19")

            assertEquals("\"${md5Hex(sourceData)}\"", firstPart.eTag())
            assertS3Error("NoSuchKey") { getObject(client, bucketName, key) }
            assertS3Error("InvalidArgument") {
                uploadPartCopy(client, bucketName, sourceKey, key, uploadId, 3, "bytes=10-${sourceData.size}")
            }

            completeMultipartUpload(client, bucketName, key, uploadId, firstPart, secondPart)

            val content = getObject(client, bucketName, key)!!.use { it.readAllBytes() }
            assertContentEquals(sourceData + sourceData.copyOfRange(10, 20), content)

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `ListMultipartUploads keeps paging after the upload named as marker was aborted`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getClient().use { client ->
            createBucket(client, bucketName)

            repeat(3) { startMultipartUpload(client, bucketName, key) }
            val allUploadIds = client.listMultipartUploads { it.bucket(bucketName) }.uploads().map { it.uploadId() }

            val firstPage = client.listMultipartUploads { it.bucket(bucketName).maxUploads(1) }
            assertEquals(allUploadIds.take(1), firstPage.uploads().map { it.uploadId() })
            client.abortMultipartUpload { it.bucket(bucketName).key(key).uploadId(allUploadIds.first()) }

            val secondPage = client.listMultipartUploads {
                it.bucket(bucketName)
                    .keyMarker(firstPage.nextKeyMarker())
                    .uploadIdMarker(firstPage.nextUploadIdMarker())
            }
            assertEquals(allUploadIds.drop(1), secondPage.uploads().map { it.uploadId() })

            assertTrue(client.listMultipartUploads { it.bucket(bucketName).maxUploads(0) }.uploads().isEmpty())

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `CompleteMultipartUpload only accepts part numbers from 1 to 10000`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getClient().use { client ->
            createBucket(client, bucketName)

            val uploadId = startMultipartUpload(client, bucketName, key)
            val part = uploadPart(client, bucketName, key, uploadId, 1, "Test".toByteArray())

            listOf(0, 10001).forEach { partNumber ->
                assertS3Error("InvalidArgument") {
                    completeMultipartUpload(
                        client, bucketName, key, uploadId,
                        CompletedPart.builder().partNumber(partNumber).eTag(part.eTag()).build()
                    )
                }
            }
            completeMultipartUpload(client, bucketName, key, uploadId, part)

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `UploadPart with a wrong Content-MD5 answers BadDigest and stores nothing`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY
        val data = "Test".toByteArray()

        getClient().use { client ->
            createBucket(client, bucketName)

            val uploadId = startMultipartUpload(client, bucketName, key)
            val request = UploadPartRequest.builder().bucket(bucketName).key(key).uploadId(uploadId).partNumber(1)

            assertS3Error("BadDigest") {
                client.uploadPart(
                    request.contentMD5(Base64.getEncoder().encodeToString(md5("Other".toByteArray()))).build(),
                    RequestBody.fromBytes(data)
                )
            }
            assertTrue(client.listParts { it.bucket(bucketName).key(key).uploadId(uploadId) }.parts().isEmpty())

            client.uploadPart(
                request.contentMD5(Base64.getEncoder().encodeToString(md5(data))).build(),
                RequestBody.fromBytes(data)
            )
            assertEquals(1, client.listParts { it.bucket(bucketName).key(key).uploadId(uploadId) }.parts().size)

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `Copying from a private bucket requires a signature even if the target is public`() {
        val privateBucketName = DEFAULT_BUCKET_NAME
        val publicBucketName = "$DEFAULT_BUCKET_NAME-public"

        getClient().use { client ->
            createBucket(client, privateBucketName)
            client.createBucket { it.bucket(publicBucketName).acl(BucketCannedACL.PUBLIC_READ_WRITE) }
            putObjectWithContent(client, privateBucketName, "secret", "Test")
            val uploadId = startMultipartUpload(client, publicBucketName, "target")
            val copySource = mapOf("x-amz-copy-source" to "/$privateBucketName/secret")

            assertEquals(403, putUnsigned("/$publicBucketName/target", copySource))
            assertEquals(403, putUnsigned("/$publicBucketName/target?partNumber=1&uploadId=$uploadId", copySource))
            assertS3Error("NoSuchKey") { getObject(client, publicBucketName, "target") }
            assertTrue(
                client.listParts { it.bucket(publicBucketName).key("target").uploadId(uploadId) }.parts().isEmpty()
            )

            client.copyObject {
                it.sourceBucket(privateBucketName).sourceKey("secret")
                    .destinationBucket(publicBucketName).destinationKey("target")
            }

            cleanupBuckets(client, privateBucketName, publicBucketName)
        }
    }

    @Test
    fun `ListMultipartUploads lists keys in the order of their UTF-8 bytes`() {
        val bucketName = DEFAULT_BUCKET_NAME
        // As Java strings, these compare the other way round, as the emoji is stored as a pair of surrogates.
        val keyInBasicPlane = "order/\uFF5E"
        val keyBeyondBasicPlane = "order/😇"

        getClient().use { client ->
            createBucket(client, bucketName)
            startMultipartUpload(client, bucketName, keyBeyondBasicPlane)
            startMultipartUpload(client, bucketName, keyInBasicPlane)

            val uploads = client.listMultipartUploads { it.bucket(bucketName) }.uploads()
            assertEquals(listOf(keyInBasicPlane, keyBeyondBasicPlane), uploads.map { it.key() })

            val secondPage = client.listMultipartUploads { it.bucket(bucketName).keyMarker(keyInBasicPlane) }
            assertEquals(listOf(keyBeyondBasicPlane), secondPage.uploads().map { it.key() })

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `CompleteMultipartUpload leaves out the parts which it does not name`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY
        val firstData = bytesOfSize(MIN_PART_SIZE)
        val lastData = "Test".toByteArray()

        getClient().use { client ->
            createBucket(client, bucketName)

            val uploadId = startMultipartUpload(client, bucketName, key)
            val firstPart = uploadPart(client, bucketName, key, uploadId, 1, firstData)
            uploadPart(client, bucketName, key, uploadId, 2, bytesOfSize(MIN_PART_SIZE))
            val lastPart = uploadPart(client, bucketName, key, uploadId, 3, lastData)
            completeMultipartUpload(client, bucketName, key, uploadId, firstPart, lastPart)

            val content = getObject(client, bucketName, key)!!.use { it.readAllBytes() }
            assertContentEquals(firstData + lastData, content)

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `ListParts answers NoSuchUpload for an unknown upload and ends the listing for a page of no parts`() {
        val bucketName = DEFAULT_BUCKET_NAME
        val key = DEFAULT_KEY

        getClient().use { client ->
            createBucket(client, bucketName)

            assertS3Error("NoSuchUpload") {
                client.listParts { it.bucket(bucketName).key(key).uploadId(UNKNOWN_UPLOAD_ID) }
            }

            val uploadId = startMultipartUpload(client, bucketName, key)
            uploadPart(client, bucketName, key, uploadId, 1, "Test".toByteArray())
            val emptyPage = client.listParts { it.bucket(bucketName).key(key).uploadId(uploadId).maxParts(0) }
            assertTrue(emptyPage.parts().isEmpty())
            assertFalse(emptyPage.isTruncated())

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `UploadPartCopy answers NoSuchUpload for an unknown upload`() {
        val bucketName = DEFAULT_BUCKET_NAME

        getClient().use { client ->
            createBucket(client, bucketName)
            putObjectWithContent(client, bucketName, "source", "Test")

            assertS3Error("NoSuchUpload") {
                uploadPartCopy(client, bucketName, "source", DEFAULT_KEY, UNKNOWN_UPLOAD_ID, 1, null)
            }

            cleanupBuckets(client, bucketName)
        }
    }

    @Test
    fun `Copy source may be encoded as a whole and name a version, but has to name an object`() {
        val bucketName = DEFAULT_BUCKET_NAME

        getClient().use { client ->
            createBucket(client, bucketName)
            putObjectWithContent(client, bucketName, "source", "Test")
            val copy = CopyObjectRequest.builder().destinationBucket(bucketName)

            client.copyObject(copy.destinationKey("encoded").copySource("$bucketName%2Fsource").build())
            assertEquals("Test", String(getObject(client, bucketName, "encoded")!!.readAllBytes()))
            client.copyObject(
                copy.destinationKey("versioned").copySource("/$bucketName/source?versionId=null").build()
            )
            assertEquals("Test", String(getObject(client, bucketName, "versioned")!!.readAllBytes()))

            assertS3Error("InvalidRequest") {
                client.copyObject(copy.destinationKey("target").copySource("/$bucketName").build())
            }
            assertS3Error("NoSuchBucket") {
                client.copyObject(copy.destinationKey("target").copySource("Not_A_Bucket/source").build())
            }
            assertS3Error("NoSuchKey") {
                client.copyObject(copy.destinationKey("target").copySource("$bucketName/..").build())
            }
            assertS3Error("NoSuchKey") { getObject(client, bucketName, "target") }

            cleanupBuckets(client, bucketName)
        }
    }

    companion object {
        const val ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
        const val SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        const val ENDPOINT = "http://localhost:9999"

        const val DEFAULT_BUCKET_NAME = "test"
        const val DEFAULT_KEY = "key/with/slashes and spaces 😇"

        const val CONTENT_DISPOSITION = "attachment; filename=\"report.pdf\""
        const val CACHE_CONTROL = "private, max-age=600"
        const val EXPIRES_HEADER = "Tue, 01 Jan 2030 00:00:00 GMT"
        val EXPIRES: Instant = Instant.parse("2030-01-01T00:00:00Z")

        /**
         * Mirrors <tt>storage.multipartMinPartSize</tt> as lowered in <tt>test.conf</tt>, so that parts stay small.
         */
        const val MIN_PART_SIZE = 1024

        /**
         * Has the form of an upload ID, but was never handed out.
         */
        val UNKNOWN_UPLOAD_ID = "0".repeat(48)

        fun doesBucketExist(client: S3Client, bucketName: String): Boolean {
            return try {
                client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build())
                true
            } catch (_: S3Exception) {
                false
            }
        }

        fun putObjectWithContent(client: S3Client, bucketName: String, key: String, content: String) {
            val data = content.toByteArray(StandardCharsets.UTF_8)

            val putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .contentLength(data.size.toLong())
                .build()

            client.putObject(putObjectRequest, RequestBody.fromBytes(data))
        }

        fun deleteObject(client: S3Client, bucketName: String, objectName: String) {
            client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(objectName).build())
        }

        fun deleteObject(client: S3AsyncClient, bucketName: String, objectName: String) {
            client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(objectName).build()).join()
        }

        fun deleteBucket(client: S3Client, bucketName: String) {
            client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build())
        }

        fun deleteBucket(client: S3AsyncClient, bucketName: String) {
            client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build()).join()
        }

        fun createBucket(client: S3Client, bucketName: String) {
            client.createBucket { it.bucket(bucketName) }
        }

        fun createBucket(client: S3AsyncClient, bucketName: String) {
            client.createBucket { it.bucket(bucketName) }.join()
        }

        fun getObject(
            client: S3Client,
            bucketName: String,
            objectName: String
        ): ResponseInputStream<GetObjectResponse?>? {
            return client.getObject(GetObjectRequest.builder().bucket(bucketName).key(objectName).build())
        }

        fun cleanupBuckets(client: S3Client, vararg bucketNames: String) {
            bucketNames.forEach { bucketName ->
                client.listObjects { it.bucket(bucketName) }.contents()?.forEach { obj ->
                    deleteObject(client, bucketName, obj.key())
                }
                deleteBucket(client, bucketName)
            }
        }

        fun cleanupBuckets(client: S3AsyncClient, vararg bucketNames: String) {
            bucketNames.forEach { bucketName ->
                client.listObjects { it.bucket(bucketName) }.get().contents()?.forEach { obj ->
                    deleteObject(client, bucketName, obj.key())
                }
                deleteBucket(client, bucketName)
            }
        }

        fun startMultipartUpload(client: S3Client, bucketName: String, key: String): String {
            return client.createMultipartUpload { it.bucket(bucketName).key(key) }.uploadId()
        }

        fun uploadPart(
            client: S3Client,
            bucketName: String,
            key: String,
            uploadId: String,
            partNumber: Int,
            data: ByteArray
        ): CompletedPart {
            val response = client.uploadPart(
                UploadPartRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId)
                    .partNumber(partNumber)
                    .contentLength(data.size.toLong())
                    .build(),
                RequestBody.fromBytes(data)
            )
            return CompletedPart.builder().partNumber(partNumber).eTag(response.eTag()).build()
        }

        fun completeMultipartUpload(
            client: S3Client,
            bucketName: String,
            key: String,
            uploadId: String,
            vararg parts: CompletedPart
        ): CompleteMultipartUploadResponse {
            return client.completeMultipartUpload {
                it.bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(*parts).build())
            }
        }

        /**
         * Creates reproducible content of the given size.
         */
        fun bytesOfSize(size: Int): ByteArray {
            return ByteArray(size) { (it % 251).toByte() }
        }

        fun md5(data: ByteArray): ByteArray {
            return MessageDigest.getInstance("MD5").digest(data)
        }

        fun md5Hex(data: ByteArray): String {
            return BaseEncoding.base16().lowerCase().encode(md5(data))
        }

        /**
         * Asserts that both HEAD and GET report the headers given when the object was uploaded.
         */
        fun assertCachingAndDispositionHeaders(client: S3Client, bucketName: String, key: String) {
            val headResponse = client.headObject { it.bucket(bucketName).key(key) }
            assertEquals(CONTENT_DISPOSITION, headResponse.contentDisposition())
            assertEquals(CACHE_CONTROL, headResponse.cacheControl())
            assertEquals(EXPIRES_HEADER, headResponse.expiresString())
            assertNotNull(headResponse.lastModified())

            val getResponse = client.getObject { it.bucket(bucketName).key(key) }.use { it.response() }
            assertEquals(CONTENT_DISPOSITION, getResponse.contentDisposition())
            assertEquals(CACHE_CONTROL, getResponse.cacheControl())
            assertEquals(EXPIRES_HEADER, getResponse.expiresString())
            assertNotNull(getResponse.lastModified())
        }

        fun uploadPartCopy(
            client: S3Client,
            bucketName: String,
            sourceKey: String,
            key: String,
            uploadId: String,
            partNumber: Int,
            range: String?
        ): CompletedPart {
            val response = client.uploadPartCopy {
                it.sourceBucket(bucketName)
                    .sourceKey(sourceKey)
                    .destinationBucket(bucketName)
                    .destinationKey(key)
                    .uploadId(uploadId)
                    .partNumber(partNumber)
                    .copySourceRange(range)
            }
            return CompletedPart.builder().partNumber(partNumber).eTag(response.copyPartResult().eTag()).build()
        }

        /**
         * Sends a PUT request without any signature and returns the status code of the response.
         */
        fun putUnsigned(path: String, headers: Map<String, String>): Int {
            val connection = URI(ENDPOINT + path).toURL().openConnection() as HttpURLConnection
            connection.requestMethod = "PUT"
            headers.forEach { (name, value) -> connection.setRequestProperty(name, value) }
            return connection.responseCode.also { connection.disconnect() }
        }

        /**
         * Asserts that the given block is refused by the server with the given S3 error code.
         */
        fun assertS3Error(expectedCode: String, block: () -> Unit) {
            val exception = assertThrows<S3Exception> { block() }
            assertEquals(expectedCode, exception.awsErrorDetails().errorCode())
        }

        fun getPresigner(): S3Presigner {
            return S3Presigner.builder()
                .credentialsProvider(
                    StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(
                            "AKIAIOSFODNN7EXAMPLE",
                            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
                        )
                    )
                )
                .serviceConfiguration(
                    S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build()
                )
                .endpointOverride(URI.create("http://localhost:9999"))
                .region(Region.EU_CENTRAL_1)
                .build()
        }
    }
}
