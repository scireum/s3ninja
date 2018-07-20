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
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import com.google.common.base.Charsets
import com.google.common.io.ByteStreams
import com.google.common.io.Files

class AWS4SignerAWSSpec extends BaseAWSSpec {

    @Override
    AmazonS3Client getClient() {
        AWSCredentials credentials = new BasicAWSCredentials(
                "AKIAIOSFODNN7EXAMPLE",
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
        AmazonS3Client newClient = new AmazonS3Client(credentials,
                                                      new ClientConfiguration())
        newClient.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true))
        newClient.setEndpoint("http://localhost:9999")

        return newClient
    }

}
