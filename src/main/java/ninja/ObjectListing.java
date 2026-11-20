/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import sirius.kernel.commons.Hasher;
import sirius.kernel.commons.Limit;
import sirius.kernel.commons.Strings;
import sirius.kernel.xml.Attribute;
import sirius.kernel.xml.XMLStructuredOutput;
import sirius.web.http.WebContext;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

/**
 * Lists the objects of a bucket the way
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html">ListObjects</a> and
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html">ListObjectsV2</a> do.
 * <p>
 * Keys are listed in the binary order of their UTF-8 bytes. With a delimiter, all keys which share the part between
 * the prefix and the next delimiter are rolled up into one common prefix, which is how clients browse a bucket like
 * a directory tree. A listing continues after a given key, which neither has to exist nor has to be an object.
 */
public class ObjectListing {

    /**
     * Contains the number of entries S3 returns at most, and by default.
     */
    private static final int MAX_KEYS = 1000;

    private static final String ENCODING_TYPE_URL = "url";
    private static final String XML_NAMESPACE = "http://s3.amazonaws.com/doc/2006-03-01/";

    /**
     * Represents an entry of a listing, which is either an object or a common prefix.
     *
     * @param name   the key of the object, or the common prefix
     * @param object the object, or <tt>null</tt> for a common prefix
     */
    private record Entry(String name, @Nullable StoredObject object) {
    }

    private final Bucket bucket;
    private final String prefix;
    private final String delimiter;
    private final int maxKeys;
    private final boolean encodeNames;

    private final List<Entry> entries = new ArrayList<>();
    private boolean truncated;

    private ObjectListing(Bucket bucket, WebContext webContext) {
        this.bucket = bucket;
        this.prefix = webContext.get("prefix").asString();
        this.delimiter = webContext.get("delimiter").asString();
        this.encodeNames = ENCODING_TYPE_URL.equals(webContext.get("encoding-type").asString());

        int requestedMaxKeys = webContext.get("max-keys").asInt(MAX_KEYS);
        this.maxKeys = requestedMaxKeys < 0 ? MAX_KEYS : Math.min(requestedMaxKeys, MAX_KEYS);
    }

    /**
     * Answers a <tt>ListObjects</tt> request.
     *
     * @param bucket     the bucket to list
     * @param webContext the request, which names what to list
     * @param output     the output to write the listing to
     */
    public static void outputVersion1(Bucket bucket, WebContext webContext, XMLStructuredOutput output) {
        ObjectListing listing = new ObjectListing(bucket, webContext);
        String marker = webContext.get("marker").asString();
        listing.collectEntries(marker);

        listing.beginOutput(output);
        output.property("Marker", listing.encode(marker));
        if (listing.truncated && Strings.isFilled(listing.delimiter)) {
            // Without a delimiter, clients continue after the last key they received.
            output.property("NextMarker", listing.encode(listing.entries.getLast().name()));
        }
        listing.outputEntries(output, true);
        output.endOutput();
    }

    /**
     * Answers a <tt>ListObjectsV2</tt> request.
     *
     * @param bucket     the bucket to list
     * @param webContext the request, which names what to list
     * @param output     the output to write the listing to
     * @throws IllegalArgumentException if the request fails {@link #hasValidContinuationToken(WebContext)}
     */
    public static void outputVersion2(Bucket bucket, WebContext webContext, XMLStructuredOutput output) {
        ObjectListing listing = new ObjectListing(bucket, webContext);
        String startAfter = webContext.get("start-after").asString();
        String continuationToken = webContext.get("continuation-token").asString();

        // A continuation token overrules start-after, which only applies to the first page.
        String continueAfter = startAfter;
        if (Strings.isFilled(continuationToken)) {
            continueAfter = decodeContinuationToken(continuationToken).orElseThrow(IllegalArgumentException::new);
        }
        listing.collectEntries(continueAfter);

        listing.beginOutput(output);
        output.property("KeyCount", listing.entries.size());
        if (Strings.isFilled(startAfter)) {
            output.property("StartAfter", listing.encode(startAfter));
        }
        if (Strings.isFilled(continuationToken)) {
            output.property("ContinuationToken", continuationToken);
        }
        if (listing.truncated) {
            output.property("NextContinuationToken", encodeContinuationToken(listing.entries.getLast().name()));
        }
        listing.outputEntries(output, webContext.get("fetch-owner").asBoolean());
        output.endOutput();
    }

    /**
     * Determines if the given <tt>ListObjectsV2</tt> request continues a listing with a usable token.
     *
     * @param webContext the request to check
     * @return <tt>false</tt> if the request carries a continuation token which has not been handed out by a previous
     * listing, <tt>true</tt> otherwise
     */
    public static boolean hasValidContinuationToken(WebContext webContext) {
        String continuationToken = webContext.get("continuation-token").asString();
        return Strings.isEmpty(continuationToken) || decodeContinuationToken(continuationToken).isPresent();
    }

    /**
     * Compares two keys by the binary order of their UTF-8 bytes, which is the order in which S3 lists keys.
     *
     * @param key1 the first key
     * @param key2 the second key
     * @return a negative number, zero or a positive number if the first key is listed before, at the same position
     * as, or after the second key
     */
    public static int compareKeys(String key1, String key2) {
        return java.util.Arrays.compareUnsigned(key1.getBytes(StandardCharsets.UTF_8),
                                                key2.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Collects the entries of one page of the listing.
     *
     * @param continueAfter the key or common prefix after which the listing continues, or an empty string to start
     *                      at the beginning
     */
    private void collectEntries(String continueAfter) {
        String lastCommonPrefix = null;
        for (StoredObject object : bucket.getObjects(null, Limit.UNLIMITED)) {
            String key = object.getKey();
            if (!key.startsWith(prefix) || (Strings.isFilled(continueAfter) && compareKeys(key, continueAfter) <= 0)) {
                continue;
            }

            String commonPrefix = determineCommonPrefix(key);
            if (commonPrefix != null && (commonPrefix.equals(lastCommonPrefix) || commonPrefix.equals(continueAfter))) {
                // Either listed already, or the previous page ended with this very prefix.
                continue;
            }

            if (entries.size() >= maxKeys) {
                truncated = maxKeys > 0;
                return;
            }

            if (commonPrefix != null) {
                entries.add(new Entry(commonPrefix, null));
                lastCommonPrefix = commonPrefix;
            } else {
                entries.add(new Entry(key, object));
            }
        }
    }

    /**
     * Determines the common prefix the given key is rolled up into.
     *
     * @return the common prefix, which ends with the delimiter, or <tt>null</tt> if the key is listed by itself
     */
    private String determineCommonPrefix(String key) {
        if (Strings.isEmpty(delimiter)) {
            return null;
        }

        int delimiterIndex = key.indexOf(delimiter, prefix.length());
        return delimiterIndex < 0 ? null : key.substring(0, delimiterIndex + delimiter.length());
    }

    private void beginOutput(XMLStructuredOutput output) {
        output.beginOutput("ListBucketResult", Attribute.set("xmlns", XML_NAMESPACE));
        output.property("Name", bucket.getName());
        output.property("Prefix", encode(prefix));
        output.property("MaxKeys", maxKeys);
        if (Strings.isFilled(delimiter)) {
            output.property("Delimiter", encode(delimiter));
        }
        if (encodeNames) {
            output.property("EncodingType", ENCODING_TYPE_URL);
        }
        output.property("IsTruncated", truncated);
    }

    private void outputEntries(XMLStructuredOutput output, boolean withOwner) {
        for (Entry entry : entries) {
            if (entry.object() == null) {
                continue;
            }

            output.beginObject("Contents");
            output.property("Key", encode(entry.name()));
            output.property("LastModified",
                            S3Dispatcher.ISO8601_INSTANT.format(entry.object().getLastModifiedInstant()));
            output.property("ETag", ObjectHandler.quoteEtag(determineEtag(entry.object())));
            output.property("Size", entry.object().getSizeBytes());
            if (withOwner) {
                output.beginObject("Owner");
                output.property("ID", "initiatorId");
                output.property("DisplayName", "initiatorName");
                output.endObject();
            }
            output.property("StorageClass", "STANDARD");
            output.endObject();
        }

        for (Entry entry : entries) {
            if (entry.object() == null) {
                output.beginObject("CommonPrefixes");
                output.property("Prefix", encode(entry.name()));
                output.endObject();
            }
        }
    }

    /**
     * Determines the entity tag of the given object.
     * <p>
     * The tag stored along with the object wins, as it is not the MD5 hash of the contents for objects which have been
     * uploaded in parts.
     */
    private String determineEtag(StoredObject object) {
        String storedEtag = object.getProperties().get(S3Dispatcher.HTTP_HEADER_NAME_ETAG);
        if (Strings.isFilled(storedEtag)) {
            return storedEtag;
        }
        return Hasher.md5().hashFile(object.getFile()).toHexString();
    }

    /**
     * Encodes a key, prefix or delimiter if the client asked for that, which permits to transport characters XML
     * cannot represent.
     */
    private String encode(String name) {
        return encodeNames && Strings.isFilled(name) ? StoredObject.encodeKey(name) : name;
    }

    private static String encodeContinuationToken(String lastKey) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(lastKey.getBytes(StandardCharsets.UTF_8));
    }

    private static Optional<String> decodeContinuationToken(String continuationToken) {
        try {
            return Optional.of(new String(Base64.getUrlDecoder().decode(continuationToken), StandardCharsets.UTF_8));
        } catch (IllegalArgumentException exception) {
            return Optional.empty();
        }
    }
}
