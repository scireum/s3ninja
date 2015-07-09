/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import sirius.kernel.Sirius;
import sirius.kernel.commons.Tuple;
import sirius.kernel.di.std.Register;
import sirius.web.templates.rythm.RythmExtension;

import java.util.function.Consumer;

/**
 * Provides additional variables to the Rythm-Context.
 */
@Register
public class NinjaExtension implements RythmExtension {
    @Override
    public void collectExtensionNames(Consumer<Tuple<String, Class<?>>> names) {
        names.accept(Tuple.create("tagLine", String.class));
        names.accept(Tuple.create("claim", String.class));
    }

    @Override
    public void collectExtensionValues(Consumer<Tuple<String, Object>> values) {
        values.accept(Tuple.create("tagLine", Sirius.getConfig().getString("product.tagLine")));
        values.accept(Tuple.create("claim", Sirius.getConfig().getString("product.claim")));
    }
}
