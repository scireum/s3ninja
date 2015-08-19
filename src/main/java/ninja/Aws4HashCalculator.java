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
     * @throws Exception in case of an unexpected error
     */
    public String computeHash(WebContext ctx) throws Exception {
        final MatchResult aws4Header = initializedMatcher(ctx);

        byte[] dateKey = hmacSHA256(("AWS4" + storage.getAwsSecretKey()).getBytes(UTF_8), aws4Header.group(2));
        byte[] dateRegionKey = hmacSHA256(dateKey, aws4Header.group(3));
        byte[] dateRegionServiceKey = hmacSHA256(dateRegionKey, "s3");
        byte[] signingKey = hmacSHA256(dateRegionServiceKey, "aws4_request");

        byte[] signedData = hmacSHA256(signingKey, buildStringToSign(ctx));
        return base16().lowerCase().encode(signedData);
    }

    private String buildStringToSign(final WebContext ctx) {
        final StringBuilder canonicalRequest = buildCanonicalRequest(ctx);
        final MatchResult aws4Header = initializedMatcher(ctx);
        return "AWS4-HMAC-SHA256\n"
               + getAmazonDateHeader(ctx)
               + "\n"
               + getAmazonDateHeader(ctx).substring(0, 8)
               + "/"
               + aws4Header.group(3)
               + "/s3/aws4_request\n"
               + hashedCanonicalRequest(canonicalRequest);
    }

    private String getAmazonDateHeader(final WebContext ctx) {
        return ctx.getHeaderValue("x-amz-date").asString();
    }

    private StringBuilder buildCanonicalRequest(final WebContext ctx) {
        final MatchResult aws4Header = initializedMatcher(ctx);
        StringBuilder canonicalRequest = new StringBuilder(ctx.getRequest().getMethod().name());
        canonicalRequest.append("\n");
        canonicalRequest.append(ctx.getRequestedURI());
        canonicalRequest.append("\n");
        canonicalRequest.append(ctx.getQueryString());
        canonicalRequest.append("\n");

        for (String name : aws4Header.group(4).split(";")) {
            canonicalRequest.append(name.trim());
            canonicalRequest.append(":");
            canonicalRequest.append(Strings.join(ctx.getRequest().headers().getAll(name), ",").trim());
            canonicalRequest.append("\n");
        }
        canonicalRequest.append("\n");
        canonicalRequest.append(aws4Header.group(4));
        canonicalRequest.append("\n");
        canonicalRequest.append(ctx.getHeader("x-amz-content-sha256"));
        return canonicalRequest;
    }

    private String hashedCanonicalRequest(final StringBuilder canonicalRequest) {
        return sha256().hashString(canonicalRequest, UTF_8).toString();
    }

    private byte[] hmacSHA256(byte[] key, String value) throws Exception {
        SecretKeySpec keySpec = new SecretKeySpec(key, "HmacSHA256");
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(keySpec);
        return mac.doFinal(value.getBytes(UTF_8));
    }
}
