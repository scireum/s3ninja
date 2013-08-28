/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import sirius.kernel.Sirius;
import sirius.kernel.commons.Collector;
import sirius.kernel.commons.Tuple;
import sirius.kernel.di.std.Register;
import sirius.web.templates.RythmExtension;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Andreas Haufler (aha@scireum.de)
 * @since 2013/08
 */
@Register
public class NinjaExtension implements RythmExtension {
    @Override
    public void collectExtensionNames(Collector<Tuple<String, Class<?>>> names) {
        names.add(new Tuple<String, Class<?>>("tagLine", String.class));
        names.add(new Tuple<String, Class<?>>("claim", String.class));
    }

    @Override
    public void collectExtensionValues(Collector<Tuple<String, Object>> values) {
        values.add(new Tuple<String, Object>("tagLine", Sirius.getConfig().getString("product.tagLine")));
        values.add(new Tuple<String, Object>("claim", Sirius.getConfig().getString("product.claim")));
    }
}
