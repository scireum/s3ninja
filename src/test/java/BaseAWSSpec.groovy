/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import com.google.common.base.Charsets
import com.google.common.io.ByteStreams
import com.google.common.io.Files
import sirius.kernel.BaseSpecification

abstract class BaseAWSSpec extends BaseSpecification {

    abstract AmazonS3Client getClient()

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
        Files.write("This is a test.", file, Charsets.UTF_8)
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
                "path/to/file",
                new ByteArrayInputStream("Test".getBytes(Charsets.UTF_8)),
                new ObjectMetadata())
        def content = new String(
                ByteStreams.toByteArray(client.getObject("test", "path/to/file").getObjectContent()),
                Charsets.UTF_8)
        then:
        content == "Test"
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
        def transfer = TransferManagerBuilder.standard().
                withS3Client(getClient()).
                withMultipartUploadThreshold(1).
                withMinimumUploadPartSize(1).build()
        def meta = new ObjectMetadata()
        def message = "Test".getBytes(Charsets.UTF_8)
        and:
        if (!getClient().doesBucketExist("test")) {
            getClient().createBucket("test")
        }
        and:
        meta.setContentLength(message.length)
        def upload = transfer.upload("test", "test", new ByteArrayInputStream("Test".getBytes(Charsets.UTF_8)), meta)
        upload.waitForUploadResult()
        def content = new String(
                ByteStreams.toByteArray(client.getObject("test", "test").getObjectContent()),
                Charsets.UTF_8)
        then:
        content == "Test"
    }

    def "MultipartUpload and then DELETE work as expected"() {
        when:
        def client = getClient()
        def transfer = TransferManagerBuilder.standard().
                withS3Client(client).
                withMultipartUploadThreshold(1).
                withMinimumUploadPartSize(1).build()
        def meta = new ObjectMetadata()
        def message = "Test".getBytes(Charsets.UTF_8)
        and:
        if (!getClient().doesBucketExist("test")) {
            getClient().createBucket("test")
        }
        and:
        meta.setContentLength(message.length)
        def upload = transfer.upload("test", "test", new ByteArrayInputStream("Test".getBytes(Charsets.UTF_8)), meta)
        upload.waitForUploadResult()
        client.deleteBucket("test")
        client.getObject("test", "test")
        then:
        AmazonS3Exception e = thrown()
        e.statusCode == 404
    }
}
