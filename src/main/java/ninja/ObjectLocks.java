/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.util.concurrent.Striped;
import sirius.kernel.di.std.Register;

import java.util.concurrent.locks.Lock;

/**
 * Provides the locks which make writing an object a single step for everyone else writing it.
 * <p>
 * An object is written by checking the conditions of the request, and then replacing its properties and its content.
 * Two requests doing so at once could both find their conditions fulfilled, which defeats <tt>If-None-Match: *</tt>
 * as a means to create an object only once, or leave the properties of one request next to the content of the other.
 */
@Register(classes = ObjectLocks.class)
public class ObjectLocks {

    /**
     * The stripes are plenty, so that two objects practically never share a lock, and are only created once needed.
     */
    private final Striped<Lock> locks = Striped.lazyWeakLock(1 << 16);

    /**
     * Returns the lock to hold while the given object is written or deleted.
     *
     * @param object the object which is about to change
     * @return the lock guarding the object
     */
    public Lock lockFor(StoredObject object) {
        return locks.get(object.getFile().getAbsolutePath());
    }
}
