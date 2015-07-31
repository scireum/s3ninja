/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */


import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.S3ClientOptions
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.transfer.TransferManager
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration
import com.google.common.base.Charsets
import com.google.common.io.ByteStreams
import sirius.kernel.BaseSpecification

class AWSSpec extends BaseSpecification {

    public AmazonS3Client getClient() {
        AWSCredentials credentials = new BasicAWSCredentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        AmazonS3Client newClient = new AmazonS3Client(credentials,
                new ClientConfiguration());
        newClient.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
        newClient.setEndpoint("http://localhost:9999/s3");

        return newClient;
    }

    def "PUT and then HEAD bucket as expected with AWS4 signer"() {
        given:
            def client = getClient();
        when:
            if (client.doesBucketExist("test")) {
                client.deleteBucket("test");
            }
            client.createBucket("test");
        then:
            client.doesBucketExist("test")
    }

    def "PUT and then DELETE bucket as expected with AWS4 signer"() {
        given:
            def client = getClient();
        when:
            if (!client.doesBucketExist("test")) {
                client.createBucket("test");
            }
            client.deleteBucket("test");
        then:
            !client.doesBucketExist("test")
    }

    def "PUT and then GET work as expected with AWS4 signer"() {
        given:
            def client = getClient();
        when:
            client.putObject("test", "test", new ByteArrayInputStream("Test".getBytes(Charsets.UTF_8)), new ObjectMetadata());
            def content = new String(ByteStreams.toByteArray(client.getObject("test", "test").getObjectContent()), Charsets.UTF_8);
        then:
            content == "Test"
    }

    def "PUT and then DELETE work as expected with AWS4 signer"() {
        given:
            def client = getClient();
        when:
            client.putObject("test", "test", new ByteArrayInputStream("Test".getBytes(Charsets.UTF_8)), new ObjectMetadata());
            client.deleteBucket("test");
            client.getObject("test", "test");
        then:
            AmazonS3Exception e = thrown();
            e.message == "Not Found (Service: Amazon S3; Status Code: 404; Error Code: 404 Not Found; Request ID: null)"
    }

    def "MultipartUpload and then GET work as expected with AWS4 signer"() {
        given:
            def client = getClient();
            def transfer = new TransferManager(client);
            def config = new TransferManagerConfiguration();
            def meta = new ObjectMetadata();
            def message = "Test".getBytes(Charsets.UTF_8);
        when:
            config.setMultipartUploadThreshold(1);
            config.setMinimumUploadPartSize(1);
            transfer.setConfiguration(config);
            meta.setContentLength(message.length);
            def upload = transfer.upload("test", "test", new ByteArrayInputStream("Test".getBytes(Charsets.UTF_8)), meta);
            upload.waitForUploadResult();
            def content = new String(ByteStreams.toByteArray(client.getObject("test", "test").getObjectContent()), Charsets.UTF_8);
        then:
            content == "Test"
    }

    def "MultipartUpload and then DELETE work as expected with AWS4 signer"() {
        given:
            def client = getClient();
            def transfer = new TransferManager(client);
            def config = new TransferManagerConfiguration();
            def meta = new ObjectMetadata();
            def message = "Test".getBytes(Charsets.UTF_8);
        when:
            config.setMultipartUploadThreshold(1);
            config.setMinimumUploadPartSize(1);
            transfer.setConfiguration(config);
            meta.setContentLength(message.length);
            def upload = transfer.upload("test", "test", new ByteArrayInputStream("Test".getBytes(Charsets.UTF_8)), meta);
            upload.waitForUploadResult();
            client.deleteBucket("test");
        client.getObject("test", "test");
        then:
            AmazonS3Exception e = thrown();
            e.message == "Not Found (Service: Amazon S3; Status Code: 404; Error Code: 404 Not Found; Request ID: null)"
    }
}
