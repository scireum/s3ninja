/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import sirius.kernel.commons.Hasher;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.Register;
import sirius.web.http.WebContext;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

/**
 * Evaluates the conditions a request can attach to reading, writing or copying an object.
 * <p>
 * These are the headers <tt>If-Match</tt>, <tt>If-None-Match</tt>, <tt>If-Modified-Since</tt> and
 * <tt>If-Unmodified-Since</tt>, and their counterparts starting with <tt>x-amz-copy-source-</tt> which refer to the
 * source of a copy. Clients use them for caching and, when writing, to avoid overwriting each other.
 */
@Register(classes = Preconditions.class)
public class Preconditions {

    /**
     * Enumerates the outcomes of evaluating the conditions of a request.
     */
    public enum Outcome {
        /**
         * All conditions hold, the request is to be performed.
         */
        PROCEED,

        /**
         * The client already holds the current state, which is answered with <tt>304 Not Modified</tt>.
         */
        NOT_MODIFIED,

        /**
         * A condition does not hold, which is answered with <tt>412 Precondition Failed</tt>.
         */
        FAILED
    }

    private static final String HEADER_IF_MATCH = "If-Match";
    private static final String HEADER_IF_NONE_MATCH = "If-None-Match";
    private static final String HEADER_IF_MODIFIED_SINCE = "If-Modified-Since";
    private static final String HEADER_IF_UNMODIFIED_SINCE = "If-Unmodified-Since";
    private static final String COPY_SOURCE_PREFIX = "x-amz-copy-source-";
    private static final String ANY_ETAG = "*";
    private static final String WEAK_ETAG_PREFIX = "W/";

    /**
     * Evaluates the conditions of a request which reads the given object.
     *
     * @param webContext the request to evaluate
     * @param object     the object to read, which has to exist
     * @return the outcome, following the precedence of RFC 7232: a matching <tt>If-Match</tt> overrules
     * <tt>If-Unmodified-Since</tt>, and <tt>If-None-Match</tt> overrules <tt>If-Modified-Since</tt>
     */
    public Outcome evaluateRead(WebContext webContext, StoredObject object) {
        return evaluate(webContext, "", object);
    }

    /**
     * Evaluates the conditions a copy request states for its source.
     *
     * @param webContext the request to evaluate
     * @param source     the object to copy from, which has to exist
     * @return {@link Outcome#PROCEED} if the source may be copied, {@link Outcome#FAILED} otherwise
     */
    public Outcome evaluateCopySource(WebContext webContext, StoredObject source) {
        return evaluate(webContext, COPY_SOURCE_PREFIX, source) == Outcome.PROCEED ? Outcome.PROCEED : Outcome.FAILED;
    }

    /**
     * Evaluates the conditions of a request which writes the given object.
     * <p>
     * S3 supports two of them: <tt>If-None-Match: *</tt> only writes an object which does not exist yet, and
     * <tt>If-Match</tt> only replaces the object if it still carries the given entity tag.
     *
     * @param webContext the request to evaluate
     * @param object     the object to write, which may or may not exist
     * @return {@link Outcome#PROCEED} if the object may be written, {@link Outcome#FAILED} otherwise
     */
    public Outcome evaluateWrite(WebContext webContext, StoredObject object) {
        String ifNoneMatch = webContext.getHeader(HEADER_IF_NONE_MATCH);
        if (Strings.isFilled(ifNoneMatch) && object.exists() && matches(ifNoneMatch, determineEtag(object), false)) {
            return Outcome.FAILED;
        }

        String ifMatch = webContext.getHeader(HEADER_IF_MATCH);
        if (Strings.isFilled(ifMatch) && (!object.exists() || !matches(ifMatch, determineEtag(object), true))) {
            return Outcome.FAILED;
        }

        return Outcome.PROCEED;
    }

    private Outcome evaluate(WebContext webContext, String headerPrefix, StoredObject object) {
        String etag = determineEtag(object);
        Instant lastModified = object.getLastModifiedInstant().truncatedTo(ChronoUnit.SECONDS);

        String ifMatch = webContext.getHeader(headerPrefix + HEADER_IF_MATCH);
        if (Strings.isFilled(ifMatch)) {
            if (!matches(ifMatch, etag, true)) {
                return Outcome.FAILED;
            }
        } else {
            Optional<Instant> unmodifiedSince =
                    parseDate(webContext.getHeader(headerPrefix + HEADER_IF_UNMODIFIED_SINCE));
            if (unmodifiedSince.isPresent() && lastModified.isAfter(unmodifiedSince.get())) {
                return Outcome.FAILED;
            }
        }

        String ifNoneMatch = webContext.getHeader(headerPrefix + HEADER_IF_NONE_MATCH);
        if (Strings.isFilled(ifNoneMatch)) {
            return matches(ifNoneMatch, etag, false) ? Outcome.NOT_MODIFIED : Outcome.PROCEED;
        }

        Optional<Instant> modifiedSince = parseDate(webContext.getHeader(headerPrefix + HEADER_IF_MODIFIED_SINCE));
        if (modifiedSince.isPresent() && !lastModified.isAfter(modifiedSince.get())) {
            return Outcome.NOT_MODIFIED;
        }

        return Outcome.PROCEED;
    }

    /**
     * Determines the entity tag of the given object, without the quotes which surround it on the wire.
     */
    private String determineEtag(StoredObject object) {
        String storedEtag = object.getProperties().get("ETag");
        if (Strings.isFilled(storedEtag)) {
            // Objects written by early versions carry their entity tag in upper case. It is sent in lower case, so
            // this is what a client states in its conditions.
            return storedEtag.toLowerCase();
        }
        return Hasher.md5().hashFile(object.getFile()).toHexString();
    }

    /**
     * Checks if the given header value, which is <tt>*</tt> or a list of entity tags, names the given entity tag.
     *
     * @param headerValue the value of the header
     * @param etag        the entity tag of the object, without quotes
     * @param strong      <tt>true</tt> to compare as <tt>If-Match</tt> demands, where a tag marked as weak never
     *                    matches and the tags have to be equal to the letter, <tt>false</tt> to compare as
     *                    <tt>If-None-Match</tt> permits, where the marker and the case of the letters are ignored
     */
    private boolean matches(String headerValue, String etag, boolean strong) {
        for (String candidate : headerValue.split(",")) {
            String normalized = candidate.trim();
            if (ANY_ETAG.equals(normalized)) {
                return true;
            }
            if (normalized.startsWith(WEAK_ETAG_PREFIX)) {
                if (strong) {
                    continue;
                }
                normalized = normalized.substring(WEAK_ETAG_PREFIX.length());
            }
            if (normalized.length() >= 2 && normalized.startsWith("\"") && normalized.endsWith("\"")) {
                normalized = normalized.substring(1, normalized.length() - 1);
            }
            if (strong ? normalized.equals(etag) : normalized.equalsIgnoreCase(etag)) {
                return true;
            }
        }
        return false;
    }

    private Optional<Instant> parseDate(String date) {
        if (Strings.isEmpty(date)) {
            return Optional.empty();
        }

        try {
            return Optional.of(ZonedDateTime.parse(date.trim(), DateTimeFormatter.RFC_1123_DATE_TIME).toInstant());
        } catch (DateTimeParseException _) {
            // As demanded by RFC 7232, a date which cannot be read is ignored.
            return Optional.empty();
        }
    }
}
