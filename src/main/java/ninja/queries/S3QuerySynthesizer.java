/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja.queries;

import ninja.Bucket;
import sirius.web.http.WebContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Permits to synthesize responses to <em>S3</em> queries such as {@code /bucket?location}.
 */
public interface S3QuerySynthesizer {

    /**
     * Synthesizes a response for the given query.
     *
     * @param ctx the request to process.
     * @param bucket the requested bucket, potentially <b>null</b>.
     * @param key the requested object's key, potentially <b>null</b>.
     * @param query the query string.
     */
    void processQuery(@Nonnull WebContext ctx,
                      @Nullable Bucket bucket,
                      @Nullable String key,
                      @Nonnull String query);
}
