/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import io.netty.handler.codec.http.QueryStringDecoder;
import sirius.kernel.commons.Monoflop;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.web.http.WebContext;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hash calculator for <a href="http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html">AWS
 * signature v4 calculation</a>
 */
@Register(classes = Aws4HashCalculator.class)
public class Aws4HashCalculator {

    protected static final Pattern AWS_AUTH4_PATTERN =
            Pattern.compile("AWS4-HMAC-SHA256 Credential=([^/]+)/([^/]+)/([^/]+)/([^/]+)/([^,]+),\\s?SignedHeaders=([^,"
                            + "]+),\\s?Signature=(.+)");

    protected static final Pattern X_AMZ_CREDENTIAL_PATTERN =
            Pattern.compile("([^/]+)/([^/]+)/([^/]+)/([^/]+)/([^,]+)");

    @Part
    private Storage storage;

    /**
     * Determines if the given request contains an AWS4 auth token.
     *
     * @param webContext the request to check
     * @return <tt>true</tt> if the request contains an AWS4 auth token, <tt>false</tt>  otherwise.
     */
    public boolean supports(final WebContext webContext) {
        return AWS_AUTH4_PATTERN.matcher(webContext.getHeaderValue("Authorization").asString("")).matches()
               || X_AMZ_CREDENTIAL_PATTERN.matcher(webContext.get("X-Amz-Credential").asString("")).matches();
    }

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
        Matcher matcher = AWS_AUTH4_PATTERN.matcher(webContext.getHeaderValue("Authorization").asString(""));

        if (!matcher.matches()) {
            // If the header doesn't match, let's try an URL parameter as we might be processing a presigned URL
            matcher = X_AMZ_CREDENTIAL_PATTERN.matcher(webContext.get("X-Amz-Credential").asString(""));
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Unknown AWS4 auth pattern");
            }
        }

        String date = matcher.group(2);
        String region = matcher.group(3);
        String service = matcher.group(4);
        String serviceType = matcher.group(5);

        // For header based requests, the signed headers are in the "Credentials" header, for presigned URLs
        // an extra parameter is given...
        String signedHeaders =
                matcher.groupCount() == 7 ? matcher.group(6) : webContext.get("X-Amz-SignedHeaders").asString();

        byte[] dateKey = hmacSHA256(("AWS4" + storage.getAwsSecretKey()).getBytes(StandardCharsets.UTF_8), date);
        byte[] dateRegionKey = hmacSHA256(dateKey, region);
        byte[] dateRegionServiceKey = hmacSHA256(dateRegionKey, service);
        byte[] signingKey = hmacSHA256(dateRegionServiceKey, serviceType);

        byte[] signedData =
                hmacSHA256(signingKey, buildStringToSign(webContext, signedHeaders, region, service, serviceType));
        return BaseEncoding.base16().lowerCase().encode(signedData);
    }

    private String buildStringToSign(final WebContext webContext,
                                     String signedHeaders,
                                     String region,
                                     String service,
                                     String serviceType) {
        final StringBuilder canonicalRequest = buildCanonicalRequest(webContext, signedHeaders);
        final String amazonDateHeader =
                webContext.getHeaderValue("x-amz-date").asString(webContext.get("X-Amz-Date").asString());
        return "AWS4-HMAC-SHA256\n"
               + amazonDateHeader
               + "\n"
               + amazonDateHeader.substring(0, 8)
               + "/"
               + region
               + "/"
               + service
               + "/"
               + serviceType
               + "\n"
               + hashedCanonicalRequest(canonicalRequest);
    }

    private StringBuilder buildCanonicalRequest(final WebContext webContext, final String signedHeaders) {
        StringBuilder canonicalRequest = new StringBuilder(webContext.getRequest().method().name());
        canonicalRequest.append("\n");
        canonicalRequest.append(webContext.getRawRequestedURI());
        canonicalRequest.append("\n");

        appendCanonicalQueryString(webContext, canonicalRequest);

        for (String name : signedHeaders.split(";")) {
            canonicalRequest.append(name.trim());
            canonicalRequest.append(":");
            canonicalRequest.append(Strings.join(webContext.getRequest().headers().getAll(name), ",").trim());
            canonicalRequest.append("\n");
        }
        canonicalRequest.append("\n");
        canonicalRequest.append(signedHeaders);
        canonicalRequest.append("\n");
        canonicalRequest.append(webContext.getHeaderValue("x-amz-content-sha256").asString("UNSIGNED-PAYLOAD"));

        return canonicalRequest;
    }

    private void appendCanonicalQueryString(WebContext webContext, StringBuilder canonicalRequest) {
        QueryStringDecoder qsd = new QueryStringDecoder(webContext.getRequest().uri(), StandardCharsets.UTF_8);

        List<Tuple<String, List<String>>> queryString = Tuple.fromMap(qsd.parameters());
        queryString.sort(Comparator.comparing(Tuple::getFirst));

        Monoflop mf = Monoflop.create();
        for (Tuple<String, List<String>> param : queryString) {
            if (!Strings.areEqual(param.getFirst(), "X-Amz-Signature")) {
                appendParam(canonicalRequest, mf, param);
            }
        }

        canonicalRequest.append("\n");
    }

    private void appendParam(StringBuilder canonicalRequest, Monoflop mf, Tuple<String, List<String>> param) {
        if (param.getSecond().isEmpty()) {
            appendQueryStringValue(param.getFirst(), "", canonicalRequest, mf.successiveCall());
        } else {
            for (String value : param.getSecond()) {
                appendQueryStringValue(param.getFirst(), value, canonicalRequest, mf.successiveCall());
            }
        }
    }

    private void appendQueryStringValue(String name,
                                        String value,
                                        StringBuilder canonicalRequest,
                                        boolean successiveCall) {
        if (successiveCall) {
            canonicalRequest.append("&");
        }
        canonicalRequest.append(Strings.urlEncode(name));
        canonicalRequest.append("=");
        canonicalRequest.append(Strings.urlEncode(value).replace("+", "%20"));
    }

    private String hashedCanonicalRequest(final StringBuilder canonicalRequest) {
        return Hashing.sha256().hashString(canonicalRequest, StandardCharsets.UTF_8).toString();
    }

    private byte[] hmacSHA256(byte[] key, String value) throws NoSuchAlgorithmException, InvalidKeyException {
        SecretKeySpec keySpec = new SecretKeySpec(key, "HmacSHA256");
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(keySpec);
        return mac.doFinal(value.getBytes(StandardCharsets.UTF_8));
    }
}
