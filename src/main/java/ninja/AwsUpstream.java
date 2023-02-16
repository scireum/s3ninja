package ninja;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.HttpMethod;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.ConfigValue;
import sirius.kernel.di.std.Register;

import java.net.URL;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Represents an upstream S3 instance which can be used in case an object is not found locally.
 * <p>
 * To enable this functionality the ConfigValue defined in this class must be set accordingly.
 * The minimal required fields are:
 * <ul>
 * <li>{@link AwsUpstream#s3EndPoint}</li>
 * <li>{@link AwsUpstream#s3AccessKey}</li>
 * <li>{@link AwsUpstream#s3SecretKey}</li>
 * </ul>
 * <p>
 * For details for the config name and expected value check each defined ConfigValue.
 */
@Register(classes = AwsUpstream.class)
public class AwsUpstream {
    private static final String FALLBACK_REGION = "EU";
    private static final int SOCKET_TIMEOUT = 60 * 1000 * 5;
    /**
     * The secret key to connect to the upstream S3 instance.
     * When this value is not set, the proxy functionality is not enabled.
     */
    @ConfigValue("upstreamAWS.secretKey")
    private String s3SecretKey;

    /**
     * The access key to connect to the upstream S3 instance.
     * When this value is not set, the proxy functionality is not enabled.
     */
    @ConfigValue("upstreamAWS.accessKey")
    private String s3AccessKey;

    /**
     * The endpoint used to connect to the upstream S3 instance.
     * When this value is not set, the proxy functionality is not enabled.
     */
    @ConfigValue("upstreamAWS.endPoint")
    private String s3EndPoint;

    /**
     * The signing region used to connect to the upstream S3 instance.
     * If not given, the value "EU" is used.
     */
    @ConfigValue("upstreamAWS.signingRegion")
    private String s3SigningRegion;

    /**
     * The signer type used to connect to the upstream S3 instance.
     * This config is optional and will be ignored if missing.
     */
    @ConfigValue("upstreamAWS.signerType")
    private String s3SignerType;

    private AmazonS3 client;

    /**
     * Checks if the (minimum) needed parameter are available to create the client.
     *
     * @return true if the minimum required config values are set.
     */
    public boolean isConfigured() {
        return Stream.of(s3EndPoint, s3AccessKey, s3SecretKey).allMatch(Strings::isFilled);
    }

    /**
     * Getter for the client instance to connect to the upstream instance.
     * Creates an instance if needed.
     *
     * @return client instance to upstream instance
     * @throws IllegalStateException if called when not configured
     */
    public AmazonS3 fetchClient() {
        if (client == null) {
            client = createAWSClient();
        }
        return client;
    }

    /**
     * @return client instance to upstream instance
     * @throws IllegalStateException if called when not configured
     */
    private AmazonS3 createAWSClient() {
        if (!isConfigured()) {
            throw new IllegalStateException("Use of not configured instance");
        }
        AWSStaticCredentialsProvider credentialsProvider =
                new AWSStaticCredentialsProvider(new BasicAWSCredentials(s3AccessKey, s3SecretKey));
        AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(
                s3EndPoint,
                Optional.ofNullable(s3SigningRegion).orElse(FALLBACK_REGION));
        ClientConfiguration config = new ClientConfiguration().withSocketTimeout(SOCKET_TIMEOUT);
        Optional.ofNullable(s3SignerType).ifPresent(config::withSignerOverride);

        return AmazonS3ClientBuilder.standard()
                                    .withClientConfiguration(config)
                                    .withPathStyleAccessEnabled(true)
                                    .withCredentials(credentialsProvider)
                                    .withEndpointConfiguration(endpointConfiguration)
                                    .build();
    }

    /**
     * Creates the url used to tunnel request to upstream instance.
     * <br><b>Important: If you do not request the content, the connection must use the method "HEAD"!</b>
     *
     * @param bucket      from which an object is fetched
     * @param object      which should be fetched
     * @param requestFile signalized if the content is needed or not
     * @return an url which can be used to perform the matching request.
     * @throws IllegalStateException if called when not configured
     */
    public URL generateGetObjectURL(Bucket bucket, StoredObject object, boolean requestFile) {
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucket.getName(), object.getKey());
        if (requestFile) {
            request.setMethod(HttpMethod.GET);
        } else {
            request.setMethod(HttpMethod.HEAD);
        }

        return fetchClient().generatePresignedUrl(request);
    }

    public void setS3SecretKey(String s3SecretKey) {
        this.s3SecretKey = s3SecretKey;
    }

    public void setS3AccessKey(String s3AccessKey) {
        this.s3AccessKey = s3AccessKey;
    }

    public void setS3EndPoint(String s3EndPoint) {
        this.s3EndPoint = s3EndPoint;
    }

    public void setS3SignerType(String s3SignerType) {
        this.s3SignerType = s3SignerType;
    }
}
