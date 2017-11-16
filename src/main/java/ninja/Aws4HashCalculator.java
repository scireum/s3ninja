/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.web.http.WebContext;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.hash.Hashing.sha256;
import static com.google.common.io.BaseEncoding.base16;

/**
 * Hash calculator for <a href="http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html">AWS
 * signature v4 calculation</a>
 */
@Register(classes = Aws4HashCalculator.class)
public class Aws4HashCalculator {

    protected static final Pattern AWS_AUTH4_PATTERN =
            Pattern.compile("AWS4-HMAC-SHA256 Credential=([^/]+)/([^/]+)/([^/]+)/s3/aws4_request, SignedHeaders=([^,"
                            + "]+), Signature=(.+)");

    @Part
    private Storage storage;

    /**
     * Determines if the given request contains an AWS4 auth token.
     *
     * @param ctx the request to check
     * @return <tt>true</tt> if the request contains an AWS4 auth token, <tt>false</tt>  otherwise.
     */
    public boolean supports(final WebContext ctx) {
        final Matcher aws4Header = buildMatcher(ctx);
        return aws4Header.matches();
    }

    private Matcher initializedMatcher(final WebContext ctx) {
        Matcher matcher = buildMatcher(ctx);
        return matcher.matches() ? matcher : null;
    }

    private Matcher buildMatcher(final WebContext ctx) {
        return AWS_AUTH4_PATTERN.matcher(ctx.getHeaderValue("Authorization").asString(""));
    }

    /**
     * Computes the authentication hash as specified by the AWS SDK for verification purposes.
     *
     * @param ctx the current request to fetch parameters from
     * @return the computes hash value
     * @throws InvalidKeyException when hashing fails
     * @throws NoSuchAlgorithmException when hashing fails
     */
    public String computeHash(WebContext ctx) throws InvalidKeyException, NoSuchAlgorithmException {
        final MatchResult aws4Header = initializedMatcher(ctx);

        byte[] dateKey = hmacSHA256(("AWS4" + storage.getAwsSecretKey()).getBytes(UTF_8), getAWS4Date(aws4Header));
        byte[] dateRegionKey = hmacSHA256(dateKey, getAWS4Region(aws4Header));
        byte[] dateRegionServiceKey = hmacSHA256(dateRegionKey, "s3");
        byte[] signingKey = hmacSHA256(dateRegionServiceKey, "aws4_request");

        byte[] signedData = hmacSHA256(signingKey, buildStringToSign(ctx, aws4Header));
        return base16().lowerCase().encode(signedData);
    }

    private String buildStringToSign(final WebContext ctx, final MatchResult aws4Header) {
        final StringBuilder canonicalRequest = buildCanonicalRequest(ctx, getAWS4SignedHeaders(aws4Header));
        final String amazonDateHeader = ctx.getHeaderValue("x-amz-date").asString();
        return "AWS4-HMAC-SHA256\n" + amazonDateHeader + "\n" + amazonDateHeader.substring(0, 8) + "/" + getAWS4Region(
                aws4Header) + "/s3/aws4_request\n" + hashedCanonicalRequest(canonicalRequest);
    }

    private StringBuilder buildCanonicalRequest(final WebContext ctx, final String signedHeaders) {
        StringBuilder canonicalRequest = new StringBuilder(ctx.getRequest().method().name());
        canonicalRequest.append("\n");
        canonicalRequest.append(ctx.getRequestedURI());
        canonicalRequest.append("\n");
        canonicalRequest.append(ctx.getQueryString());
        canonicalRequest.append("\n");

        for (String name : signedHeaders.split(";")) {
            canonicalRequest.append(name.trim());
            canonicalRequest.append(":");
            canonicalRequest.append(Strings.join(ctx.getRequest().headers().getAll(name), ",").trim());
            canonicalRequest.append("\n");
        }
        canonicalRequest.append("\n");
        canonicalRequest.append(signedHeaders);
        canonicalRequest.append("\n");
        canonicalRequest.append(ctx.getHeader("x-amz-content-sha256"));
        return canonicalRequest;
    }

    private String getAWS4Date(MatchResult aws4Header) {
        return aws4Header.group(2);
    }

    private String getAWS4Region(MatchResult aws4Header) {
        return aws4Header.group(3);
    }

    private String getAWS4SignedHeaders(MatchResult aws4Header) {
        return aws4Header.group(4);
    }

    private String hashedCanonicalRequest(final StringBuilder canonicalRequest) {
        return sha256().hashString(canonicalRequest, UTF_8).toString();
    }

    private byte[] hmacSHA256(byte[] key, String value) throws NoSuchAlgorithmException, InvalidKeyException {
        SecretKeySpec keySpec = new SecretKeySpec(key, "HmacSHA256");
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(keySpec);
        return mac.doFinal(value.getBytes(UTF_8));
    }
}
