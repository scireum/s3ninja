/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.collect.Lists;
import sirius.kernel.cache.Cache;
import sirius.kernel.cache.CacheManager;
import sirius.kernel.cache.ValueComputer;
import sirius.kernel.health.Exceptions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Represents a bucket.
 * <p>
 * Internally a bucket is just a directory within the base directory.
 * </p>
 */
public class Bucket {

    private File file;
    private static Cache<String, Boolean> publicAccessCache = CacheManager.createCache("public-bucket-access");

    /**
     * Creates a new bucket based on the given directory.
     *
     * @param file the directory which stores the contents of the bucket.
     */
    public Bucket(File file) {
        this.file = file;
    }

    /**
     * Returns the name of the bucket.
     *
     * @return the name of the bucket
     */
    public String getName() {
        return file.getName();
    }

    /**
     * Deletes the bucket and all of its contents.
     */
    public void delete() {
        for (File child : file.listFiles()) {
            child.delete();
        }
        file.delete();
    }

    /**
     * Creates the bucket.
     * <p>
     * If the underlying directory already exists, nothing happens.
     * </p>
     */
    public void create() {
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    /**
     * Returns a list of all stored objects
     *
     * @return a list of all objects in the bucket.
     */
    public List<StoredObject> getObjects() {
        List<StoredObject> result = Lists.newArrayList();
        for (File child : file.listFiles()) {
            if (child.isFile() && !child.getName().startsWith("__")) {
                result.add(new StoredObject(child));
            }
        }

        return result;
    }

    /**
     * Determines if the bucket is private or public accessible
     *
     * @return <tt>true</tt> if the bucket is public accessible, <tt>false</tt> otherwise
     */
    public boolean isPrivate() {
        return !publicAccessCache.get(getName(), new ValueComputer<String, Boolean>() {
            @Nullable
            @Override
            public Boolean compute(@Nonnull String key) {
                return getPublicMarkerFile().exists();
            }
        });
    }

    private File getPublicMarkerFile() {
        return new File(file, "__ninja_public");
    }

    /**
     * Marks the bucket as private accessible.
     */
    public void makePrivate() {
        if (getPublicMarkerFile().exists()) {
            getPublicMarkerFile().delete();
            publicAccessCache.put(getName(), false);
        }
    }

    /**
     * Marks the bucket as public accessible.
     */
    public void makePublic() {
        if (!getPublicMarkerFile().exists()) {
            try {
                new FileOutputStream(getPublicMarkerFile()).close();
            } catch (IOException e) {
                throw Exceptions.handle(Storage.LOG, e);
            }
        }
        publicAccessCache.put(getName(), true);
    }

    /**
     * Returns the underlying directory as File.
     *
     * @return a <tt>File</tt> representing the underlying directory
     */
    public File getFile() {
        return file;
    }

    /**
     * Determines if the bucket exists.
     *
     * @return <tt>true</tt> if the bucket exists, <tt>false</tt> otherwise
     */
    public boolean exists() {
        return file.exists();
    }

    /**
     * Returns the child object with the given id.
     *
     * @param id the name of the requested child object. Must not contain .. / or \
     * @return the object with the given id, might not exist, but is always non null
     */
    public StoredObject getObject(String id) {
        if (id.contains("..") || id.contains("/") || id.contains("\\")) {
            throw Exceptions.createHandled()
                            .withSystemErrorMessage(
                                    "Invalid object name: %s. A object name must not contain '..' '/' or '\\'",
                                    id)
                            .handle();
        }
        return new StoredObject(new File(file, id));
    }
}
