/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

import com.amazonaws.HttpMethod
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.Headers
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.model.CannedAccessControlList
import com.amazonaws.services.s3.model.CreateBucketRequest
import com.amazonaws.services.s3.model.DeleteObjectsRequest
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.ResponseHeaderOverrides
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import com.google.common.io.ByteStreams
import com.google.common.io.Files
import sirius.kernel.BaseSpecification

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.temporal.ChronoUnit

abstract class BaseAWSSpec extends BaseSpecification {

    def DEFAULT_BUCKET_NAME = "test"

    def DEFAULT_KEY = "key/with/slashes and spaces 😇"

    abstract AmazonS3Client getClient()

    private void putObjectWithContent(String bucketName, String key, String content) {
        def client = getClient()
        def data = content.getBytes(StandardCharsets.UTF_8)

        def metadata = new ObjectMetadata()
        metadata.setHeader(Headers.CONTENT_LENGTH, new Long(data.length))

        client.putObject(bucketName, key, new ByteArrayInputStream(data), metadata)
    }

    def "HEAD of non-existing bucket as expected"() {
        given:
        def bucketName = "does-not-exist"
        def client = getClient()
        when:
        if (client.doesBucketExist(bucketName)) {
            client.deleteBucket(bucketName)
        }
        then:
        !client.doesBucketExist(bucketName)
        !client.doesBucketExistV2(bucketName)
    }

    def "PUT and then HEAD bucket as expected"() {
        given:
        def bucketName = DEFAULT_BUCKET_NAME
        def client = getClient()
        when:
        if (client.doesBucketExist(bucketName)) {
            client.deleteBucket(bucketName)
        }
        client.createBucket(bucketName)
        then:
        client.doesBucketExist(bucketName)
        client.doesBucketExistV2(bucketName)
    }

    def "DELETE of non-existing bucket as expected"() {
        given:
        def bucketName = "does-not-exist"
        def client = getClient()
        when:
        if (client.doesBucketExist(bucketName)) {
            client.deleteBucket(bucketName)
        }
        and:
        client.deleteBucket(bucketName)
        then:
        AmazonS3Exception e = thrown()
        e.statusCode == 404
        !client.doesBucketExist(bucketName)
        !client.doesBucketExistV2(bucketName)
    }

    def "PUT and then DELETE bucket as expected"() {
        given:
        def bucketName = DEFAULT_BUCKET_NAME
        def client = getClient()
        when:
        if (!client.doesBucketExist(bucketName)) {
            client.createBucket(bucketName)
        }
        client.deleteBucket(bucketName)
        then:
        !client.doesBucketExist(bucketName)
    }

    def "PUT and then GET file work using TransferManager"() {
        given:
        def bucketName = DEFAULT_BUCKET_NAME
        def key = DEFAULT_KEY
        def client = getClient()
        when:
        if (!client.doesBucketExist(bucketName)) {
            client.createBucket(bucketName)
        }
        and:
        File file = File.createTempFile("test", "")
        file.delete()
        for (int i = 0; i < 10000; i++) {
            Files.append("This is a test.", file, StandardCharsets.UTF_8)
        }
        and:
        def tm = TransferManagerBuilder.standard().withS3Client(client).build()
        tm.upload(bucketName, key, file).waitForUploadResult()
        and:
        File download = File.createTempFile("s3-test", "")
        download.deleteOnExit()
        tm.download(bucketName, key, download).waitForCompletion()
        then:
        Files.toString(file, StandardCharsets.UTF_8) == Files.toString(download, StandardCharsets.UTF_8)
        and:
        client.deleteObject(bucketName, key)
    }

    def "PUT and then GET work as expected"() {
        given:
        def bucketName = DEFAULT_BUCKET_NAME
        def key = DEFAULT_KEY
        def client = getClient()
        when:
        if (!client.doesBucketExist(bucketName)) {
            client.createBucket(bucketName)
        }
        and:
        putObjectWithContent(bucketName, key, "Test")
        def content = new String(
                ByteStreams.toByteArray(client.getObject(bucketName, key).getObjectContent()),
                StandardCharsets.UTF_8)
        and:
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucketName, key)
        URLConnection c = new URL(getClient().generatePresignedUrl(request).toString()).openConnection()
        and:
        String downloadedData = new String(ByteStreams.toByteArray(c.getInputStream()), StandardCharsets.UTF_8)
        then:
        content == "Test"
        and:
        downloadedData == "Test"
        and:
        client.deleteObject(bucketName, key)
    }

    def "PUT and then LIST work as expected"() {
        given:
        def bucketName = DEFAULT_BUCKET_NAME
        def key1 = DEFAULT_KEY + "/Eins"
        def key2 = DEFAULT_KEY + "/Zwei"
        def client = getClient()
        when:
        if (client.doesBucketExist(bucketName)) {
            client.deleteBucket(bucketName)
        }
        client.createBucket(bucketName)
        and:
        putObjectWithContent(bucketName, key1, "Eins")
        putObjectWithContent(bucketName, key2, "Zwei")
        then:
        def listing = client.listObjects(bucketName)
        def summaries = listing.getObjectSummaries()
        summaries.size() == 2
        summaries.get(0).getKey() == key1
        summaries.get(1).getKey() == key2
    }

    // reported in https://github.com/scireum/s3ninja/issues/180
    def "PUT and then LIST with prefix work as expected"() {
        given:
        def bucketName = DEFAULT_BUCKET_NAME
        def key1 = DEFAULT_KEY + "/Eins"
        def key2 = DEFAULT_KEY + "/Zwei"
        def key3 = "a/key/with a different/prefix/Drei"
        def client = getClient()
        when:
        if (client.doesBucketExist(bucketName)) {
            client.deleteBucket(bucketName)
        }
        client.createBucket(bucketName)
        and:
        putObjectWithContent(bucketName, key1, "Eins")
        putObjectWithContent(bucketName, key2, "Zwei")
        putObjectWithContent(bucketName, key3, "Drei")
        then:
        def listing = client.listObjects(bucketName, DEFAULT_KEY + '/')
        def summaries = listing.getObjectSummaries()
        summaries.size() == 2
        summaries.get(0).getKey() == key1
        summaries.get(1).getKey() == key2
    }

    def "PUT and then DELETE work as expected"() {
        given:
        def bucketName = DEFAULT_BUCKET_NAME
        def key = DEFAULT_KEY
        def client = getClient()
        when:
        if (!client.doesBucketExist(bucketName)) {
            client.createBucket(bucketName)
        }
        and:
        putObjectWithContent(bucketName, key, "Test")
        client.deleteBucket(bucketName)
        client.getObject(bucketName, key)
        then:
        AmazonS3Exception e = thrown()
        e.statusCode == 404
    }

    def "MultipartUpload and then GET work as expected"() {
        given:
        def bucketName = DEFAULT_BUCKET_NAME
        def key = DEFAULT_KEY
        def client = getClient()
        when:
        def transfer = TransferManagerBuilder.standard().
                withS3Client(client).
                withMultipartUploadThreshold(1).
                withMinimumUploadPartSize(1).build()
        def meta = new ObjectMetadata()
        def message = "Test".getBytes(StandardCharsets.UTF_8)
        and:
        if (!client.doesBucketExist(bucketName)) {
            client.createBucket(bucketName)
        }
        and:
        meta.setContentLength(message.length)
        meta.addUserMetadata("userdata", "test123")
        def upload = transfer.upload(bucketName, key, new ByteArrayInputStream(message), meta)
        upload.waitForUploadResult()
        def content = new String(
                ByteStreams.toByteArray(client.getObject(bucketName, key).getObjectContent()),
                StandardCharsets.UTF_8)
        def userdata = client.getObjectMetadata(bucketName, key).getUserMetaDataOf("userdata")
        then:
        content == "Test"
        userdata == "test123"
        and:
        client.deleteObject(bucketName, key)
    }

    def "MultipartUpload and then DELETE work as expected"() {
        given:
        def bucketName = DEFAULT_BUCKET_NAME
        def key = DEFAULT_KEY
        def client = getClient()
        when:
        def transfer = TransferManagerBuilder.standard().
                withS3Client(client).
                withMultipartUploadThreshold(1).
                withMinimumUploadPartSize(1).build()
        def meta = new ObjectMetadata()
        def message = "Test".getBytes(StandardCharsets.UTF_8)
        and:
        if (!client.doesBucketExist(bucketName)) {
            client.createBucket(bucketName)
        }
        and:
        meta.setContentLength(message.length)
        def upload = transfer.upload(bucketName, key, new ByteArrayInputStream(message), meta)
        upload.waitForUploadResult()
        client.deleteBucket(bucketName)
        client.getObject(bucketName, key)
        then:
        AmazonS3Exception e = thrown()
        e.statusCode == 404
    }

    def "PUT on presigned URL without signed chunks works as expected"() {
        given:
        def bucketName = DEFAULT_BUCKET_NAME
        def key = DEFAULT_KEY
        def client = getClient()
        when:
        if (!client.doesBucketExist(bucketName)) {
            client.createBucket(bucketName)
        }
        and:
        def content = "NotSigned"
        and:
        GeneratePresignedUrlRequest putRequest = new GeneratePresignedUrlRequest(bucketName, key, HttpMethod.PUT)
        HttpURLConnection hc = new URL(getClient().generatePresignedUrl(putRequest).toString()).openConnection()
        hc.setDoOutput(true)
        hc.setRequestMethod("PUT")
        OutputStreamWriter out = new OutputStreamWriter(hc.getOutputStream())
        try {
            out.write(content)
        } finally {
            out.close()
        }
        hc.getResponseCode()
        and:
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucketName, key)
        URLConnection c = new URL(getClient().generatePresignedUrl(request).toString()).openConnection()
        and:
        String downloadedData = new String(ByteStreams.toByteArray(c.getInputStream()), StandardCharsets.UTF_8)
        then:
        downloadedData == content
        and:
        client.deleteObject(bucketName, key)
    }

    // reported in https://github.com/scireum/s3ninja/issues/153
    def "PUT and then GET on presigned URL with ResponseHeaderOverrides works as expected"() {
        given:
        def bucketName = DEFAULT_BUCKET_NAME
        def key = DEFAULT_KEY
        def client = getClient()
        when:
        if (!client.doesBucketExist(bucketName)) {
            client.createBucket(bucketName)
        }
        and:
        putObjectWithContent(bucketName, key, "Test")
        def content = new String(
                ByteStreams.toByteArray(client.getObject(bucketName, key).getObjectContent()),
                StandardCharsets.UTF_8)
        and:
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucketName, key)
                .withExpiration(Date.from(Instant.now().plus(1, ChronoUnit.HOURS)))
                .withResponseHeaders(
                        new ResponseHeaderOverrides()
                                .withContentDisposition("inline; filename=\"hello.txt\""))
        URLConnection c = new URL(getClient().generatePresignedUrl(request).toString()).openConnection()
        and:
        String downloadedData = new String(ByteStreams.toByteArray(c.getInputStream()), StandardCharsets.UTF_8)
        then:
        content == "Test"
        and:
        downloadedData == "Test"
        and:
        client.deleteObject(bucketName, key)
    }

    // reported in https://github.com/scireum/s3ninja/issues/181
    def "Bulk delete using DeleteObjectCommand works as expected"() {
        given:
        def bucketName = DEFAULT_BUCKET_NAME
        def key1 = DEFAULT_KEY + "/Eins"
        def key2 = DEFAULT_KEY + "/Zwei"
        def key3 = DEFAULT_KEY + "/Drei"
        def client = getClient()
        when:
        putObjectWithContent(bucketName, key1, "Eins")
        putObjectWithContent(bucketName, key2, "Zwei")
        putObjectWithContent(bucketName, key3, "Drei")
        def result = client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(key1, key2))
        then:
        result.getDeletedObjects().size() == 2
        result.getDeletedObjects().get(0).getKey() == key1
        result.getDeletedObjects().get(1).getKey() == key2
        and:
        def listing = client.listObjects(bucketName)
        listing.getObjectSummaries().size() == 1
        listing.getObjectSummaries().get(0).getKey() == key3
        and:
        client.deleteObject(bucketName, key3)
    }

    // reported in https://github.com/scireum/s3ninja/issues/214
    def "ListObjectsV2 works as expected"() {
        given:
        def bucketName = DEFAULT_BUCKET_NAME
        def key1 = DEFAULT_KEY + "/Eins"
        def key2 = DEFAULT_KEY + "/Eins-Eins"
        def key3 = DEFAULT_KEY + "/Drei"
        def client = getClient()
        when:
        putObjectWithContent(bucketName, key1, "Eins")
        putObjectWithContent(bucketName, key2, "Zwei")
        putObjectWithContent(bucketName, key3, "Drei")
        def result = client.listObjectsV2(new ListObjectsV2Request().withBucketName(bucketName).withPrefix(key1))
        then:
        result.getKeyCount() == 2
        result.getObjectSummaries().size() == 2
        result.getObjectSummaries().get(0).getKey() == key1
        result.getObjectSummaries().get(1).getKey() == key2
        and:
        client.deleteObject(bucketName, key1)
        client.deleteObject(bucketName, key2)
        client.deleteObject(bucketName, key3)
    }

    // reported in https://github.com/scireum/s3ninja/issues/209
    def "HEAD reports content length correctly"() {
        given:
        def bucketName = "public-bucket"
        def key = "simple_test"
        def content = "I am pointless text content"
        def client = getClient()
        when:
        client.createBucket(new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.PublicReadWrite))
        putObjectWithContent(bucketName, key, content)
        then:
        def url = new URL("http://localhost:9999/" + bucketName + "/" + key)
        HttpURLConnection connection = (HttpURLConnection) url.openConnection()
        connection.setRequestMethod("HEAD")
        connection.getResponseCode() == 200
        connection.getContentLengthLong() == content.getBytes(StandardCharsets.UTF_8).length
        connection.disconnect()
        and:
        client.deleteObject(bucketName, key)
        client.deleteBucket(bucketName)
    }

    // reported in https://github.com/scireum/s3ninja/issues/230
    def "Copying an object within the same bucket works as expected"() {
        given:
        def bucketName = DEFAULT_BUCKET_NAME
        def keyFrom = DEFAULT_KEY
        def keyTo = keyFrom + "-copy"
        def content = "I am pointless text content, but I deserve to exist twice and will thus be copied!"
        def client = getClient()
        when:
        if (!client.doesBucketExist(bucketName)) {
            client.createBucket(bucketName)
        }
        and:
        putObjectWithContent(bucketName, keyFrom, content)
        and:
        client.copyObject(bucketName, keyFrom, bucketName, keyTo);
        and:
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucketName, keyTo)
        URLConnection c = new URL(getClient().generatePresignedUrl(request).toString()).openConnection()
        and:
        String downloadedData = new String(ByteStreams.toByteArray(c.getInputStream()), StandardCharsets.UTF_8)
        then:
        downloadedData == content
        and:
        client.deleteObject(bucketName, keyFrom)
        client.deleteObject(bucketName, keyTo)
        client.deleteBucket(bucketName)
    }

    // reported in https://github.com/scireum/s3ninja/issues/230
    def "Copying an object across buckets works as expected"() {
        given:
        def bucketNameFrom = DEFAULT_BUCKET_NAME
        def bucketNameTo = DEFAULT_BUCKET_NAME + "-copy"
        def key = DEFAULT_KEY
        def content = "I am pointless text content, but I deserve to exist twice and will thus be copied!"
        def client = getClient()
        when:
        if (!client.doesBucketExist(bucketNameFrom)) {
            client.createBucket(bucketNameFrom)
        }
        and:
        if (!client.doesBucketExist(bucketNameTo)) {
            client.createBucket(bucketNameTo)
        }
        and:
        putObjectWithContent(bucketNameFrom, key, content)
        and:
        client.copyObject(bucketNameFrom, key, bucketNameTo, key);
        and:
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucketNameTo, key)
        URLConnection c = new URL(getClient().generatePresignedUrl(request).toString()).openConnection()
        and:
        String downloadedData = new String(ByteStreams.toByteArray(c.getInputStream()), StandardCharsets.UTF_8)
        then:
        downloadedData == content
        and:
        client.deleteObject(bucketNameFrom, key)
        client.deleteBucket(bucketNameFrom)
        client.deleteObject(bucketNameTo, key)
        client.deleteBucket(bucketNameTo)
    }
}
