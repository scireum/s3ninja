import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.S3ClientOptions
import ninja.AwsUpstream
import org.junit.AfterClass
import org.junit.Assert
import org.junit.BeforeClass
import sirius.kernel.di.Injector

import java.time.LocalDateTime
import java.util.concurrent.TimeUnit

class S3ProxySpec extends BaseAWSSpec {

    private static Process upstreamS3Ninja

    private static AwsUpstream awsUpstream

    private static AmazonS3Client upstreamClient

    private static final def port = 10000
    private static final def secretKey = "AKIAIOSFODNN7EXAMPLE" + new Random().nextInt()
    private static final def accessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" + new Random().nextInt()
    private static final def endPoint = "http://localhost:" + port + "/s3"
    private static final def signerType = "S3SignerType"
    private static final def storage = File.createTempDir("s3-test-upstream-instance", "")

    private static final def bucketName = "proxy-test"

    @Override
    AmazonS3Client getClient() {
        return createClient("AKIAIOSFODNN7EXAMPLE",
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "http://localhost:9999/s3")
    }

    static
    AmazonS3Client createClient(accessKey, secretKey, endpoint) {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey)
        AmazonS3Client newClient = new AmazonS3Client(credentials, new ClientConfiguration().withSignerOverride("S3SignerType"))
        newClient.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(true).build())
        newClient.setEndpoint(endpoint)
        return newClient
    }

    @BeforeClass
    static
    def "Start 'upstream' instance"() {
        upstreamClient = createClient(accessKey, secretKey, endPoint)
        awsUpstream = Injector.context().getPart(AwsUpstream)
        awsUpstream.s3SecretKey = secretKey
        awsUpstream.s3AccessKey = accessKey
        awsUpstream.s3EndPoint = endPoint
        awsUpstream.s3SignerType = signerType

        Assert.assertTrue(awsUpstream.isConfigured())
        println "Init 'upstream' s3-ninja"

        String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java"
        String classpath = System.getProperty("java.class.path")
        ProcessBuilder builder = new ProcessBuilder(javaBin, "-cp", classpath, "sirius.kernel.Setup")
        builder.environment().put("http.port", String.valueOf(port))
        builder.environment().put("storage.awsAccessKey", accessKey)
        builder.environment().put("storage.awsSecretKey", secretKey)
        builder.environment().put("storage.baseDir", storage.absolutePath)
        upstreamS3Ninja = builder.start()

        boolean waitForServer = true
        def waitServerTimeout = LocalDateTime.now().plusSeconds(10)
        while (waitForServer && waitServerTimeout.isAfter(LocalDateTime.now())) {
            try {
                upstreamClient.doesBucketExist(bucketName)
                waitForServer = false
            } catch (Throwable ignored) {
                println "Waiting for 'upstream' s3-ninja to come online"
                sleep(100)
            }
        }
        Assert.assertFalse("Wait for 'upstream' s3-ninja timed out", waitForServer)
        if (upstreamS3Ninja.isAlive()) {
            println "Started 'upstream' s3-ninja, storage dir: " + storage.absolutePath
        } else {
            println "'upstream' s3-ninja exited prematurely, exit code: " + upstreamS3Ninja.exitValue()
        }
        Assert.assertTrue(upstreamS3Ninja.isAlive())
    }

    @AfterClass
    static
    def "Stop 'upstream' instance"() {
        awsUpstream.s3SecretKey = null
        awsUpstream.s3AccessKey = null
        awsUpstream.s3EndPoint = null
        awsUpstream.s3SignerType = null
        if (upstreamS3Ninja != null) {
            upstreamS3Ninja.destroy()
            println "'upstream' s3-ninja exited stopped: " + upstreamS3Ninja.waitFor(1, TimeUnit.MINUTES)
        }
    }

    def "Object exists neither local or remote"() {
        given:
        def objectName = "missing-object"
        expect:
        !upstreamClient.doesObjectExist(bucketName, objectName)
        !client.doesObjectExist(bucketName, objectName)
    }

    def "Get upstream only object"() {
        given:
        def objectName = "upstream-only-object"
        def content = "This does now exist."
        when:
        upstreamClient.deleteObject(bucketName, objectName)
        then:
        !upstreamClient.doesObjectExist(bucketName, objectName)
        !client.doesObjectExist(bucketName, objectName)
        when:
        upstreamClient.putObject(bucketName, objectName, content)
        then:
        upstreamClient.doesObjectExist(bucketName, objectName)
        client.doesObjectExist(bucketName, objectName)
        content == client.getObjectAsString(bucketName, objectName)
    }

    def "Delete locally only, upstream must be untouched"() {
        given:
        def objectName = "delete-only-locally"
        def upstreamContent = "This does now exist. Upstream"
        when:
        // cleanup existing files
        upstreamClient.deleteObject(bucketName, objectName)
        client.deleteObject(bucketName, objectName)

        upstreamClient.putObject(bucketName, objectName, upstreamContent)
        then:
        upstreamContent == client.getObjectAsString(bucketName, objectName)
        when:
        client.deleteObject(bucketName, objectName)
        then:
        upstreamContent == upstreamClient.getObjectAsString(bucketName, objectName)
        !client.doesObjectExist(bucketName, objectName)
    }

    def "Overwrite locally only, upstream must be untouched"() {
        given:
        def objectName = "overwrite-locally"
        def upstreamContent = "This does now exist. Upstream"
        def localContent = "This does now exist. Locally"
        when:
        // cleanup existing files
        upstreamClient.deleteObject(bucketName, objectName)
        client.deleteObject(bucketName, objectName)

        upstreamClient.putObject(bucketName, objectName, upstreamContent)
        then:
        upstreamContent == upstreamClient.getObjectAsString(bucketName, objectName)
        upstreamContent == client.getObjectAsString(bucketName, objectName)
        when:
        client.putObject(bucketName, objectName, localContent)
        then:
        upstreamContent == upstreamClient.getObjectAsString(bucketName, objectName)
        localContent == client.getObjectAsString(bucketName, objectName)
    }

    def "Locally only object"() {
        given:
        def objectName = "exists-only-locally"
        def localContent = "This does now exist. Locally"
        when:
        client.putObject(bucketName, objectName, localContent)
        then:
        !upstreamClient.doesObjectExist(bucketName, objectName)
        localContent == client.getObjectAsString(bucketName, objectName)
    }


    def "Overwrite locally deleted file"() {
        given:
        def objectName = "overwrite-locally-deleted"
        def upstreamContent = "This does now exist. Upstream"
        def localContent = "This does now exist. Locally"
        when:
        // cleanup existing files
        upstreamClient.deleteObject(bucketName, objectName)
        client.deleteObject(bucketName, objectName)

        upstreamClient.putObject(bucketName, objectName, upstreamContent)
        then:
        upstreamContent == upstreamClient.getObjectAsString(bucketName, objectName)
        upstreamContent == client.getObjectAsString(bucketName, objectName)
        when:
        client.deleteObject(bucketName, objectName)
        then:
        upstreamContent == upstreamClient.getObjectAsString(bucketName, objectName)
        !client.doesObjectExist(bucketName, objectName)
        when:
        client.putObject(bucketName, objectName, localContent)
        then:
        upstreamContent == upstreamClient.getObjectAsString(bucketName, objectName)
        localContent == client.getObjectAsString(bucketName, objectName)
    }
}
