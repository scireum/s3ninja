/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja.queries;

import ninja.Bucket;
import sirius.web.http.WebContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Permits to process <em>S3</em> queries such as {@code /bucket?location}.
 */
public interface S3QueryProcessor {

    /**
     * Processes the given query.
     *
     * @param webContext the request to process
     * @param bucket     the requested bucket, potentially <b>null</b>
     * @param key        the requested object's key, potentially <b>null</b>
     * @param query      the query string
     */
    void processQuery(@Nonnull WebContext webContext,
                      @Nonnull Bucket bucket,
                      @Nullable String key,
                      @Nonnull String query);
}
