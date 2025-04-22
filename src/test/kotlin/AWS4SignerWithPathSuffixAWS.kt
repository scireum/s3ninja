import org.junit.jupiter.api.extension.ExtendWith
import sirius.kernel.SiriusExtension
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration
import java.net.URI

@ExtendWith(SiriusExtension::class)
class AWS4SignerWithPathSuffixAWS : BaseAWS() {

    private val endpointWithSuffix = "$ENDPOINT/s3"
    override fun getClient(): S3Client {
        return S3Client.builder()
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        ACCESS_KEY,
                        SECRET_ACCESS_KEY
                    )
                )
            )
            .serviceConfiguration(
                S3Configuration.builder()
                    .pathStyleAccessEnabled(true)
                    .build()
            )
            .endpointOverride(URI.create(endpointWithSuffix))
            .region(Region.EU_CENTRAL_1)
            .build()
    }

    override fun getAsyncClient(): S3AsyncClient {
        return S3AsyncClient.builder()
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        ACCESS_KEY,
                        SECRET_ACCESS_KEY
                    )
                )
            )
            .serviceConfiguration(
                S3Configuration.builder()
                    .pathStyleAccessEnabled(true)
                    .build()
            )
            .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
            .endpointOverride(URI.create(endpointWithSuffix))
            .region(Region.EU_CENTRAL_1)
            .build()
    }
}
