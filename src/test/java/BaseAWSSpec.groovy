/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

import com.amazonaws.HttpMethod
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.ResponseHeaderOverrides
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import com.google.common.base.Charsets
import com.google.common.io.ByteStreams
import com.google.common.io.Files
import sirius.kernel.BaseSpecification

import java.time.Instant
import java.time.temporal.ChronoUnit

abstract class BaseAWSSpec extends BaseSpecification {

    abstract AmazonS3Client getClient()

    def "HEAD of non-existing bucket as expected"() {
        given:
        def client = getClient()
        when:
        if (client.doesBucketExist("does-not-exist")) {
            client.deleteBucket("does-not-exist")
        }
        then:
        !client.doesBucketExist("does-not-exist")
        !client.doesBucketExistV2("does-not-exist")
    }

    def "PUT and then HEAD bucket as expected"() {
        given:
        def client = getClient()
        when:
        if (client.doesBucketExist("test")) {
            client.deleteBucket("test")
        }
        client.createBucket("test")
        then:
        client.doesBucketExist("test")
        client.doesBucketExistV2("test")
    }

    def "DELETE of non-existing bucket as expected"() {
        given:
        def client = getClient()
        when:
        if (client.doesBucketExist("does-not-exist")) {
            client.deleteBucket("does-not-exist")
        }
        and:
        client.deleteBucket("does-not-exist")
        then:
        AmazonS3Exception e = thrown()
        e.statusCode == 404
        !client.doesBucketExist("does-not-exist")
        !client.doesBucketExistV2("does-not-exist")
    }

    def "PUT and then DELETE bucket as expected"() {
        given:
        def client = getClient()
        when:
        if (!client.doesBucketExist("test")) {
            client.createBucket("test")
        }
        client.deleteBucket("test")
        then:
        !client.doesBucketExist("test")
    }

    def "PUT and then GET file work using TransferManager"() {
        when:
        def client = getClient()
        if (!client.doesBucketExist("test")) {
            client.createBucket("test")
        }
        and:
        File file = File.createTempFile("test", "")
        file.delete()
        for (int i = 0; i < 10000; i++) {
            Files.append("This is a test.", file, Charsets.UTF_8)
        }
        and:
        def tm = TransferManagerBuilder.standard().withS3Client(client).build()
        tm.upload("test", "test", file).waitForUploadResult()
        and:
        File download = File.createTempFile("s3-test", "")
        download.deleteOnExit()
        tm.download("test", "test", download).waitForCompletion()
        then:
        Files.toString(file, Charsets.UTF_8) == Files.toString(download, Charsets.UTF_8)
    }

    def "PUT and then GET work as expected"() {
        given:
        def client = getClient()
        when:
        if (!client.doesBucketExist("test")) {
            client.createBucket("test")
        }
        and:
        client.putObject(
                "test",
                "test",
                new ByteArrayInputStream("Test".getBytes(Charsets.UTF_8)),
                new ObjectMetadata())
        def content = new String(
                ByteStreams.toByteArray(client.getObject("test", "test").getObjectContent()),
                Charsets.UTF_8)
        and:
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest("test", "test")
        URLConnection c = new URL(getClient().generatePresignedUrl(request).toString()).openConnection()
        and:
        String downloadedData = new String(ByteStreams.toByteArray(c.getInputStream()), Charsets.UTF_8)
        then:
        content == "Test"
        and:
        downloadedData == "Test"
    }

    def "PUT and then LIST work as expected"() {
        given:
        def client = getClient()
        when:
        if (client.doesBucketExist("test")) {
            client.deleteBucket("test")
        }
        client.createBucket("test")
        and:
        client.putObject(
                "test",
                "Eins",
                new ByteArrayInputStream("Eins".getBytes(Charsets.UTF_8)),
                new ObjectMetadata())
        client.putObject(
                "test",
                "Zwei",
                new ByteArrayInputStream("Zwei".getBytes(Charsets.UTF_8)),
                new ObjectMetadata())
        then:
        def listing = client.listObjects("test")
        def summaries = listing.getObjectSummaries()
        summaries.size() == 2
        summaries.get(0).getKey() == "Eins"
        summaries.get(1).getKey() == "Zwei"
    }

    def "PUT and then DELETE work as expected"() {
        given:
        def client = getClient()
        when:
        if (!client.doesBucketExist("test")) {
            client.createBucket("test")
        }
        and:
        client.putObject(
                "test",
                "test",
                new ByteArrayInputStream("Test".getBytes(Charsets.UTF_8)),
                new ObjectMetadata())
        client.deleteBucket("test")
        client.getObject("test", "test")
        then:
        AmazonS3Exception e = thrown()
        e.statusCode == 404
    }

    def "MultipartUpload and then GET work as expected"() {
        when:
        def bucketName = "test"
        def key = "key/with/slashes"
        def client = getClient()
        def transfer = TransferManagerBuilder.standard().
                withS3Client(client).
                withMultipartUploadThreshold(1).
                withMinimumUploadPartSize(1).build()
        def meta = new ObjectMetadata()
        def message = "Test".getBytes(Charsets.UTF_8)
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
                Charsets.UTF_8)
        def userdata = client.getObjectMetadata(bucketName, key).getUserMetaDataOf("userdata")
        then:
        content == "Test"
        userdata == "test123"
    }

    def "MultipartUpload and then DELETE work as expected"() {
        when:
        def bucketName = "test"
        def key = "key/with/slashes"
        def client = getClient()
        def transfer = TransferManagerBuilder.standard().
                withS3Client(client).
                withMultipartUploadThreshold(1).
                withMinimumUploadPartSize(1).build()
        def meta = new ObjectMetadata()
        def message = "Test".getBytes(Charsets.UTF_8)
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
        def client = getClient()
        when:
        if (!client.doesBucketExist("test")) {
            client.createBucket("test")
        }
        and:
        def content = "NotSigned"
        and:
        GeneratePresignedUrlRequest putRequest = new GeneratePresignedUrlRequest("test", "test", HttpMethod.PUT)
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
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest("test", "test")
        URLConnection c = new URL(getClient().generatePresignedUrl(request).toString()).openConnection()
        and:
        String downloadedData = new String(ByteStreams.toByteArray(c.getInputStream()), Charsets.UTF_8)
        then:
        downloadedData == content
    }

    // reported in https://github.com/scireum/s3ninja/issues/153
    def "PUT and then GET on presigned URL with ResponseHeaderOverrides works as expected"() {
        given:
        def client = getClient()
        when:
        if (!client.doesBucketExist("test")) {
            client.createBucket("test")
        }
        and:
        client.putObject(
                "test",
                "test",
                new ByteArrayInputStream("Test".getBytes(Charsets.UTF_8)),
                new ObjectMetadata())
        def content = new String(
                ByteStreams.toByteArray(client.getObject("test", "test").getObjectContent()),
                Charsets.UTF_8)
        and:
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest("test", "test")
                .withExpiration(Date.from(Instant.now().plus(1, ChronoUnit.HOURS)))
                .withResponseHeaders(
                        new ResponseHeaderOverrides()
                                .withContentDisposition("inline; filename=\"hello.txt\""))
        URLConnection c = new URL(getClient().generatePresignedUrl(request).toString()).openConnection()
        and:
        String downloadedData = new String(ByteStreams.toByteArray(c.getInputStream()), Charsets.UTF_8)
        then:
        content == "Test"
        and:
        downloadedData == "Test"
    }
}
