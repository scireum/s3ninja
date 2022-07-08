/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import io.netty.handler.codec.http.HttpHeaders;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.web.http.WebContext;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import static sirius.kernel.commons.Strings.join;

/**
 * Hash calculator for legacy AWS signature calculation
 */
@Register(classes = AwsLegacyHashCalculator.class)
public class AwsLegacyHashCalculator {

    @Part
    private Storage storage;

    private static final List<String> SIGNED_PARAMETERS = Arrays.asList("acl",
                                                                        "torrent",
                                                                        "logging",
                                                                        "location",
                                                                        "policy",
                                                                        "requestPayment",
                                                                        "versioning",
                                                                        "versions",
                                                                        "versionId",
                                                                        "notification",
                                                                        "uploadId",
                                                                        "uploads",
                                                                        "partNumber",
                                                                        "website",
                                                                        "delete",
                                                                        "lifecycle",
                                                                        "tagging",
                                                                        "cors",
                                                                        "restore",
                                                                        "response-content-type",
                                                                        "response-content-language",
                                                                        "response-expires",
                                                                        "response-cache-control",
                                                                        "response-content-disposition",
                                                                        "response-content-encoding");

    /**
     * Computes the authentication hash as specified by the AWS SDK for verification purposes.
     *
     * @param webContext the current request to fetch parameters from
     * @param pathPrefix the path prefix to preped to the {@link S3Dispatcher#getEffectiveURI(String) effective URI}
     *                   of the request
     * @return the computes hash value
     * @throws Exception when hashing fails
     */
    public String computeHash(WebContext webContext, String pathPrefix) throws Exception {
        StringBuilder stringToSign = new StringBuilder(webContext.getRequest().method().name());
        stringToSign.append("\n");
        stringToSign.append(webContext.getHeaderValue("Content-MD5").asString(""));
        stringToSign.append("\n");
        stringToSign.append(webContext.getHeaderValue("Content-Type").asString(""));
        stringToSign.append("\n");

        String date = webContext.get("Expires").asString(webContext.getHeaderValue("Date").asString(""));
        if (webContext.getHeaderValue("x-amz-date").isNull()) {
            stringToSign.append(date);
        }
        stringToSign.append("\n");

        HttpHeaders requestHeaders = webContext.getRequest().headers();
        List<String> headers = requestHeaders.names()
                                             .stream()
                                             .filter(this::relevantAmazonHeader)
                                             .map(name -> toHeaderStringRepresentation(name, requestHeaders))
                                             .sorted()
                                             .toList();

        for (String header : headers) {
            stringToSign.append(header);
            stringToSign.append("\n");
        }

        stringToSign.append(pathPrefix)
                    .append('/')
                    .append(S3Dispatcher.getEffectiveURI(webContext.getRawRequestedURI()));

        char separator = '?';
        for (String parameterName : webContext.getParameterNames().stream().sorted().toList()) {
            // Skip parameters that aren't part of the canonical signed string
            if (SIGNED_PARAMETERS.contains(parameterName)) {
                stringToSign.append(separator).append(parameterName);
                String parameterValue = webContext.get(parameterName).asString();
                if (Strings.isFilled(parameterValue)) {
                    stringToSign.append("=").append(parameterValue);
                }
                separator = '&';
            }
        }

        // Caution: Do not blindly copy this example as the AWS secret is passed as string.
        // Strings are immutable in Java and stay in memory until collected by Java's garbage collector.
        // Thus, the JCA recommends the usage of a char array <https://docs.oracle.com/javase/8/docs/technotes/guides/security/crypto/CryptoSpec.html#PBEEx>.
        SecretKeySpec keySpec = new SecretKeySpec(storage.getAwsSecretKey().getBytes(), "HmacSHA1");
        Mac mac = Mac.getInstance("HmacSHA1");
        mac.init(keySpec);
        byte[] result = mac.doFinal(stringToSign.toString().getBytes(StandardCharsets.UTF_8.name()));
        return Base64.getEncoder().encodeToString(result);
    }

    private boolean relevantAmazonHeader(final String name) {
        return name.toLowerCase().startsWith("x-amz-");
    }

    private String toHeaderStringRepresentation(final String headerName, final HttpHeaders requestHeaders) {
        return headerName.toLowerCase().trim() + ":" + join(requestHeaders.getAll(headerName), ",").trim();
    }
}
