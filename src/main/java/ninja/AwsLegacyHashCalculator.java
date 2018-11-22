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
import com.google.common.io.BaseEncoding;
import io.netty.handler.codec.http.HttpHeaders;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.web.http.WebContext;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

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
     * Computes the authentication hash as specified by the AWS SDK for verification purposes, one with the "/s3" path
     * prefix and one without it.
     *
     * @param ctx the current request to fetch parameters from
     * @return the computed hash values
     * @throws NoSuchAlgorithmException     when hashing fails
     * @throws InvalidKeyException          when hashing fails
     * @throws UnsupportedEncodingException when hashing fails
     */
    public Collection<String> computeHash(final WebContext ctx)
            throws NoSuchAlgorithmException, InvalidKeyException, UnsupportedEncodingException {
        return ImmutableSet.of(computeHash(ctx, ctx.getRequestedURI()), computeHash(ctx, getEffectiveURI(ctx)));
    }

    private String computeHash(final WebContext ctx, String uri)
            throws NoSuchAlgorithmException, InvalidKeyException, UnsupportedEncodingException {
        StringBuilder stringToSign = new StringBuilder(ctx.getRequest().method().name());
        stringToSign.append("\n");
        stringToSign.append(ctx.getHeaderValue("Content-MD5").asString(""));
        stringToSign.append("\n");
        stringToSign.append(ctx.getHeaderValue("Content-Type").asString(""));
        stringToSign.append("\n");

        String date = ctx.get("Expires").asString(ctx.getHeaderValue("Date").asString(""));
        if (ctx.getHeaderValue("x-amz-date").isNull()) {
            stringToSign.append(date);
        }
        stringToSign.append("\n");

        HttpHeaders requestHeaders = ctx.getRequest().headers();
        List<String> headers = requestHeaders.names()
                                             .stream()
                                             .filter(this::relevantAmazonHeader)
                                             .map(name -> toHeaderStringRepresentation(name, requestHeaders))
                                             .sorted()
                                             .collect(Collectors.toList());

        for (String header : headers) {
            stringToSign.append(header);
            stringToSign.append("\n");
        }

        stringToSign.append(uri);

        char separator = '?';
        for (String parameterName : ctx.getParameterNames().stream().sorted().collect(Collectors.toList())) {
            // Skip parameters that aren't part of the canonical signed string
            if (SIGNED_PARAMETERS.contains(parameterName)) {
                stringToSign.append(separator).append(parameterName);
                String parameterValue = ctx.get(parameterName).asString();
                if (Strings.isFilled(parameterValue)) {
                    stringToSign.append("=").append(parameterValue);
                }
                separator = '&';
            }
        }

        SecretKeySpec keySpec = new SecretKeySpec(storage.getAwsSecretKey().getBytes(), "HmacSHA1");
        Mac mac = Mac.getInstance("HmacSHA1");
        mac.init(keySpec);
        byte[] result = mac.doFinal(stringToSign.toString().getBytes(Charsets.UTF_8.name()));
        return BaseEncoding.base64().encode(result);
    }

    private String getEffectiveURI(WebContext ctx) {
        String uri = ctx.getRequestedURI();
        if (uri.startsWith("/s3")) {
            uri = uri.substring(3);
        }

        return uri;
    }

    private boolean relevantAmazonHeader(final String name) {
        return name.toLowerCase().startsWith("x-amz-");
    }

    private String toHeaderStringRepresentation(final String headerName, final HttpHeaders requestHeaders) {
        return headerName.toLowerCase().trim() + ":" + join(requestHeaders.getAll(headerName), ",").trim();
    }
}
