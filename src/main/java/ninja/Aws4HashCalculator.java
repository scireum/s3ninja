/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import io.netty.handler.codec.http.QueryStringDecoder;
import sirius.kernel.commons.Monoflop;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.web.http.WebContext;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Hash calculator for <a href="http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html">AWS
 * signature v4 calculation</a>
 */
@Register(classes = Aws4HashCalculator.class)
public class Aws4HashCalculator {

    public static final String HEADER_AUTHORIZATION = "Authorization";
    public static final String UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";
    public static final String SHA256_EMPTY = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    protected static final Pattern AWS_AUTH4_PATTERN = Pattern.compile(
            "AWS4-HMAC-SHA256 Credential=([^/]+)/([^/]+)/([^/]+)/([^/]+)/aws4_request, SignedHeaders=([^,]+), Signature=(.+)");

    protected static final Pattern AWS_AUTH4_CREDENTIAL_PATTERN =
            Pattern.compile("([^/]+)/([^/]+)/([^/]+)/([^/]+)/aws4_request");

    @Part
    private Storage storage;

    /**
     * Determines if the given request contains an AWS4 auth token.
     *
     * @param ctx the request to check
     * @return <tt>true</tt> if the request contains an AWS4 auth token, <tt>false</tt>  otherwise.
     */
    public boolean supports(final WebContext ctx) {
        return AWS_AUTH4_PATTERN.matcher(ctx.getHeaderValue(HEADER_AUTHORIZATION).asString("")).matches()
               || "AWS4-HMAC-SHA256".equalsIgnoreCase(ctx.get("X-Amz-Algorithm").asString());
    }

    /**
     * Computes the authentication hash as specified by the AWS SDK for verification purposes, one for the AWS4Signer
     * and one for the AWS3V4Signer.
     *
     * @param ctx the current request to fetch parameters from
     * @return the computed hash values
     * @throws InvalidKeyException      when hashing fails
     * @throws NoSuchAlgorithmException when hashing fails
     * @throws IOException              in case of an IO error
     */
    public Collection<String> computeHash(WebContext ctx)
            throws InvalidKeyException, NoSuchAlgorithmException, IOException {
        boolean isPresignedUrl = ctx.getHeaderValue(HEADER_AUTHORIZATION).isEmptyString();

        Matcher matcher = isPresignedUrl ?
                          AWS_AUTH4_CREDENTIAL_PATTERN.matcher(ctx.get("X-Amz-Credential").asString("")) :
                          AWS_AUTH4_PATTERN.matcher(ctx.getHeaderValue(HEADER_AUTHORIZATION).asString(""));
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Unknown AWS4 auth pattern");
        }

        String signedHeaders =
                (isPresignedUrl ? ctx.get("X-Amz-SignedHeaders").asString("") : matcher.group(5)).toLowerCase();
        String date = matcher.group(2);
        String awsRegion = matcher.group(3);
        String awsService = matcher.group(4);

        byte[] dateKey = hmacSHA256(("AWS4" + storage.getAwsSecretKey()).getBytes(Charsets.UTF_8), date);
        byte[] dateRegionKey = hmacSHA256(dateKey, awsRegion);
        byte[] dateRegionServiceKey = hmacSHA256(dateRegionKey, awsService);
        byte[] signingKey = hmacSHA256(dateRegionServiceKey, "aws4_request");

        String credential = date + "/" + awsRegion + "/" + awsService + "/aws4_request";

        return ImmutableSet.of(computeHash(ctx, signingKey, signedHeaders, credential, isPresignedUrl, false),
                               computeHash(ctx, signingKey, signedHeaders, credential, isPresignedUrl, true));
    }

    private String computeHash(final WebContext ctx,
                               byte[] signingKey,
                               String signedHeaders,
                               String credential,
                               boolean isPresignedUrl,
                               boolean v4) throws NoSuchAlgorithmException, InvalidKeyException, IOException {
        String amazonDateHeader = ctx.getHeaderValue("x-amz-date").asString(ctx.get("X-Amz-Date").asString(""));
        String canonicalRequest = buildCanonicalRequest(ctx, signedHeaders, isPresignedUrl, v4);
        String stringToSign =
                "AWS4-HMAC-SHA256\n" + amazonDateHeader + "\n" + credential + "\n" + hex(sha256(canonicalRequest));
        return hex(hmacSHA256(signingKey, stringToSign));
    }

    private String buildCanonicalRequest(final WebContext ctx,
                                         final String signedHeaders,
                                         boolean preSignedUrl,
                                         boolean v4) throws IOException {
        StringBuilder canonicalRequest = new StringBuilder();
        canonicalRequest.append(ctx.getRequest().method().name());
        canonicalRequest.append("\n");
        canonicalRequest.append(ctx.getRequestedURI());
        canonicalRequest.append("\n");

        appendCanonicalQueryString(ctx, canonicalRequest, preSignedUrl);
        appendSignedHeaderValues(ctx, canonicalRequest, signedHeaders);

        canonicalRequest.append("\n");
        canonicalRequest.append(signedHeaders);
        canonicalRequest.append("\n");

        appendPayloadHash(ctx, canonicalRequest, preSignedUrl, v4);

        return canonicalRequest.toString();
    }

    private void appendCanonicalQueryString(final WebContext ctx,
                                            StringBuilder canonicalRequest,
                                            boolean preSignedUrl) {
        QueryStringDecoder qsd = new QueryStringDecoder(ctx.getRequest().uri(), Charsets.UTF_8);

        List<Tuple<String, List<String>>> queryString = Tuple.fromMap(qsd.parameters());
        queryString.sort(Comparator.comparing(Tuple::getFirst));

        Monoflop mf = Monoflop.create();
        for (Tuple<String, List<String>> param : queryString) {
            if (preSignedUrl && "X-Amz-Signature".equalsIgnoreCase(param.getFirst())) {
                continue;
            }
            if (param.getSecond().isEmpty()) {
                appendQueryStringValue(canonicalRequest, param.getFirst(), "", mf.successiveCall());
            } else {
                for (String value : param.getSecond()) {
                    appendQueryStringValue(canonicalRequest, param.getFirst(), value, mf.successiveCall());
                }
            }
        }

        canonicalRequest.append("\n");
    }

    private void appendQueryStringValue(StringBuilder canonicalRequest,
                                        String name,
                                        String value,
                                        boolean successiveCall) {
        if (successiveCall) {
            canonicalRequest.append("&");
        }
        canonicalRequest.append(Strings.urlEncode(name));
        canonicalRequest.append("=");
        canonicalRequest.append(Strings.urlEncode(value));
    }

    private void appendSignedHeaderValues(final WebContext ctx, StringBuilder canonicalRequest, String signedHeaders) {
        Stream.of(signedHeaders.split(";"))
              .sorted()
              .map(name -> name.trim().toLowerCase() + ":" + Strings.join(ctx.getRequest().headers().getAll(name), ",")
                                                                    .trim() + "\n")
              .forEach(canonicalRequest::append);
    }

    private void appendPayloadHash(final WebContext ctx,
                                   StringBuilder canonicalRequest,
                                   boolean preSignedUrl,
                                   boolean v4) throws IOException {
        if (preSignedUrl && !v4) {
            canonicalRequest.append(UNSIGNED_PAYLOAD);
        } else {
            // TODO doesn't work for predispatched requests with AWS4Signer because ctx.getContent() is empty
            canonicalRequest.append(ctx.getHeaderValue("x-amz-content-sha256")
                                       .asString(ctx.hasContent() ?
                                                 sha256(ByteStreams.toByteArray(ctx.getContent())) :
                                                 SHA256_EMPTY));
        }
    }

    private byte[] hmacSHA256(byte[] key, String value) throws NoSuchAlgorithmException, InvalidKeyException {
        SecretKeySpec keySpec = new SecretKeySpec(key, "HmacSHA256");
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(keySpec);
        return mac.doFinal(value.getBytes(Charsets.UTF_8));
    }

    private String hex(byte[] data) {
        return BaseEncoding.base16().lowerCase().encode(data);
    }

    private String sha256(byte[] data) {
        return Hashing.sha256().hashBytes(data).toString();
    }

    private byte[] sha256(String data) {
        return Hashing.sha256().hashString(data, Charsets.UTF_8).asBytes();
    }
}
