/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.BaseEncoding;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builds presigned POST payloads (policy + signature) for the UI helper.
 */
@Register(classes = PresignedPostService.class)
public class PresignedPostService {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final DateTimeFormatter DATE_FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.of("UTC"));
    private static final DateTimeFormatter DATE_TIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneId.of("UTC"));

    @Part
    private Storage storage;
    /**
     * Generate presigned POST DTO and concatenate all the below fields.
     *  "key"
     *  "policy"
     *  "x-amz-algorithm"
     *  "x-amz-credential"
     *  "x-amz-date"
     *  "x-amz-signature",
     *  "x-amz-security-token"
     *
     */
    public PresignedPostResponse generate(PresignedPostRequest request) {
        validate(request);

        Instant now = Instant.now();
        Instant expiration = now.plus(Duration.ofHours(request.expirationHours()));
        String date = DATE_FORMAT.format(now);
        String xAmzDate = DATE_TIME_FORMAT.format(now);
        String credential = request.accessKey() + "/" + date + "/" + request.region() + "/s3/aws4_request";

        ObjectNode policyJson = buildPolicy(request, credential, xAmzDate, expiration);
        String policyString = policyJson.toString();
        String policyBase64 = BaseEncoding.base64().encode(policyString.getBytes(StandardCharsets.UTF_8));
        String signature = signPolicy(policyString.getBytes(StandardCharsets.UTF_8), date, request.region());

        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("key", request.key());
        fields.put("policy", policyBase64);
        fields.put("x-amz-algorithm", "AWS4-HMAC-SHA256");
        fields.put("x-amz-credential", credential);
        fields.put("x-amz-date", xAmzDate);
        fields.put("x-amz-signature", signature);
        if (request.sessionToken() != null) {
            fields.put("x-amz-security-token", request.sessionToken());
        }

        return new PresignedPostResponse("/" + request.bucket() + "/" + request.key(),
                                         fields,
                                         policyString,
                                         policyBase64,
                                         signature);
    }

    private void validate(PresignedPostRequest request) {
        if (request.expirationHours() < 1 || request.expirationHours() > 168) {
            throw Exceptions.createHandled().withDirectMessage("Expiration hours must be between 1 and 168").handle();
        }
    }

    private ObjectNode buildPolicy(PresignedPostRequest request,
                                   String credential,
                                   String xAmzDate,
                                   Instant expiration) {
        ObjectNode root = OBJECT_MAPPER.createObjectNode();
        root.put("expiration", expiration.toString());

        ArrayNode conditions = root.putArray("conditions");
        conditions.add(OBJECT_MAPPER.createObjectNode().put("bucket", request.bucket()));
        ArrayNode keyCondition = OBJECT_MAPPER.createArrayNode();
        keyCondition.add("eq");
        keyCondition.add("$key");
        keyCondition.add(request.key());
        conditions.add(keyCondition);
        conditions.add(OBJECT_MAPPER.createObjectNode().put("x-amz-algorithm", "AWS4-HMAC-SHA256"));
        conditions.add(OBJECT_MAPPER.createObjectNode().put("x-amz-credential", credential));
        conditions.add(OBJECT_MAPPER.createObjectNode().put("x-amz-date", xAmzDate));
        if (request.sessionToken() != null) {
            conditions.add(OBJECT_MAPPER.createObjectNode().put("x-amz-security-token", request.sessionToken()));
        }

        return root;
    }

    private String signPolicy(byte[] policy, String date, String region) {
        try {
            byte[] kSecret = ("AWS4" + storage.getAwsSecretKey()).getBytes(StandardCharsets.UTF_8);
            byte[] kDate = hmac(kSecret, date);
            byte[] kRegion = hmac(kDate, region);
            byte[] kService = hmac(kRegion, "s3");
            byte[] kSigning = hmac(kService, "aws4_request");
            return BaseEncoding.base16().lowerCase().encode(hmac(kSigning, policy));
        } catch (Exception e) {
            throw Exceptions.handle(e);
        }
    }

    private byte[] hmac(byte[] key, String value) throws Exception {
        return hmac(key, value.getBytes(StandardCharsets.UTF_8));
    }

    private byte[] hmac(byte[] key, byte[] value) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        return mac.doFinal(value);
    }
}

