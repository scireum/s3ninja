/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

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
import java.time.Duration
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

    companion object {
        const val ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
        const val SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        const val ENDPOINT = "http://localhost:9999"

        const val DEFAULT_BUCKET_NAME = "test"
        const val DEFAULT_KEY = "key/with/slashes and spaces 😇"

        /**
         * Mirrors <tt>storage.multipartMinPartSize</tt> as lowered in <tt>test.conf</tt>, so that parts stay small.
         */
        const val MIN_PART_SIZE = 1024

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
