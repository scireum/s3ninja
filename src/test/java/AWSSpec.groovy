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
import com.amazonaws.services.s3.model.ObjectMetadata
import com.google.common.base.Charsets
import com.google.common.io.ByteStreams
import sirius.kernel.BaseSpecification


class AWSSpec extends BaseSpecification {

    public static AmazonS3Client getClient() {
        AWSCredentials credentials = new BasicAWSCredentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        AmazonS3Client newClient = new AmazonS3Client(credentials,
                new ClientConfiguration().withSignerOverride());
        newClient.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
        newClient.setEndpoint("http://localhost:9444/s3");

        return newClient;
    }

    def "PUT and then GET work as expected"() {
        given:
        AmazonS3Client client = getClient();
        when:
        client.putObject("test", "test", new ByteArrayInputStream("Test".getBytes(Charsets.UTF_8)), new ObjectMetadata());
        def content = new String(ByteStreams.toByteArray(client.getObject("test", "test").getObjectContent()), Charsets.UTF_8);
        then:
        content == "Test"
    }
}
