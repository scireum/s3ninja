/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import sirius.kernel.Sirius;
import sirius.kernel.di.std.Register;
import sirius.web.templates.rythm.RythmExtension;

import java.util.function.BiConsumer;

/**
 * Provides additional variables to the Rythm-Context.
 */
@Register
public class NinjaExtension implements RythmExtension {

    @Override
    public void collectExtensionNames(BiConsumer<String, Class<?>> names) {
        names.accept("tagLine", String.class);
        names.accept("claim", String.class);
    }

    @Override
    public void collectExtensionValues(BiConsumer<String, Object> values) {
        values.accept("tagLine", Sirius.getConfig().getString("product.tagLine"));
        values.accept("claim", Sirius.getConfig().getString("product.claim"));
    }
}
