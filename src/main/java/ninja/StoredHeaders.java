/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.collect.Maps;
import sirius.kernel.commons.Strings;
import sirius.web.http.WebContext;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Knows which headers of an upload are stored along with the object and sent again when it is read.
 * <p>
 * S3 keeps a fixed set of standard headers, the user metadata and a few of its own headers which describe the object.
 * Everything else belongs to the request and not to the object: the date, the signature, the hash of the content and
 * the like must not come back when the object is read.
 */
public final class StoredHeaders {

    private static final String USER_METADATA_PREFIX = "x-amz-meta-";
    private static final String CHECKSUM_PREFIX = "x-amz-checksum-";
    private static final String CONTENT_ENCODING = "content-encoding";

    /**
     * Names the encoding by which SDKs transfer an upload in signed chunks. It describes the transfer and not the
     * object, which is why S3 removes it from the stored <tt>Content-Encoding</tt>.
     */
    private static final String TRANSFER_ENCODING_AWS_CHUNKED = "aws-chunked";

    private static final Set<String> STANDARD_HEADERS = Set.of("cache-control",
                                                               "content-disposition",
                                                               CONTENT_ENCODING,
                                                               "content-language",
                                                               "content-type",
                                                               "expires");

    private static final Set<String> AMAZON_HEADERS = Set.of("x-amz-storage-class",
                                                             "x-amz-server-side-encryption",
                                                             "x-amz-website-redirect-location",
                                                             CHECKSUM_PREFIX + "crc32",
                                                             CHECKSUM_PREFIX + "crc32c",
                                                             CHECKSUM_PREFIX + "sha1",
                                                             CHECKSUM_PREFIX + "sha256");

    private StoredHeaders() {
    }

    /**
     * Collects the headers of the given upload request which are to be stored along with the object.
     *
     * @param webContext the request which creates the object
     * @return the headers to store, by their names as sent by the client
     */
    public static Map<String, String> ofRequest(WebContext webContext) {
        Map<String, String> headers = Maps.newTreeMap();
        for (String name : webContext.getRequest().headers().names()) {
            String value = webContext.getHeader(name);
            if (CONTENT_ENCODING.equalsIgnoreCase(name)) {
                value = removeTransferEncoding(value);
            }
            if (isStored(name) && Strings.isFilled(value)) {
                headers.put(name, value);
            }
        }
        return headers;
    }

    /**
     * Determines if the header with the given name is stored along with an object.
     * <p>
     * This is also asked when an object is read, so that objects which have been stored by a previous version do not
     * send the request headers which used to be kept.
     *
     * @param name the name of the header, in any case
     * @return <tt>true</tt> if the header describes the object, <tt>false</tt> if it belongs to the request
     */
    public static boolean isStored(String name) {
        String lowerCaseName = name.toLowerCase();
        return STANDARD_HEADERS.contains(lowerCaseName)
               || AMAZON_HEADERS.contains(lowerCaseName)
               || lowerCaseName.startsWith(USER_METADATA_PREFIX);
    }

    /**
     * Determines if the header with the given name carries a checksum of the whole object.
     * <p>
     * Such a header must not be sent along with a part of the object, as clients verify what they receive against it.
     *
     * @param name the name of the header, in any case
     * @return <tt>true</tt> if the header is a checksum of the object, <tt>false</tt> otherwise
     */
    public static boolean isChecksum(String name) {
        return name.toLowerCase().startsWith(CHECKSUM_PREFIX);
    }

    private static String removeTransferEncoding(String contentEncoding) {
        if (Strings.isEmpty(contentEncoding)) {
            return contentEncoding;
        }

        return Arrays.stream(contentEncoding.split(","))
                     .map(String::trim)
                     .filter(encoding -> !TRANSFER_ENCODING_AWS_CHUNKED.equalsIgnoreCase(encoding))
                     .collect(Collectors.joining(", "));
    }
}
