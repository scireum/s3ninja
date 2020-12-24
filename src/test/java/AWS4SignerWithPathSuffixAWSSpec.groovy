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

class AWS4SignerWithPathSuffixAWSSpec extends BaseAWSSpec {

    @Override
    AmazonS3Client getClient() {
        AWSCredentials credentials = new BasicAWSCredentials(
                "AKIAIOSFODNN7EXAMPLE",
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
        AmazonS3Client newClient = new AmazonS3Client(credentials,
                new ClientConfiguration())
        newClient.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(true).build())
        newClient.setEndpoint("http://localhost:9999/s3")

        return newClient
    }

}
