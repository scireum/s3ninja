/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import sirius.kernel.health.Exceptions;
import sirius.kernel.nls.NLS;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Represents a stored object.
 */
public class StoredObject {
    private File file;

    /**
     * Creates a new StoredObject based on a file.
     *
     * @param file the contents of the object.
     */
    public StoredObject(File file) {
        this.file = file;
    }

    /**
     * Returns the name of the object.
     *
     * @return the name of the object
     */
    public String getName() {
        return file.getName();
    }

    /**
     * Returns the size of the object.
     *
     * @return a string representation of the byte-size of the object
     */
    public String getSize() {
        return NLS.formatSize(file.length());
    }

    /**
     * Returns the last modified date of the object
     *
     * @return a string representation of the last modification date
     */
    public String getLastModified() {
        return NLS.toUserString(getLastModifiedInstant());
    }

    public Instant getLastModifiedInstant() {
        return Instant.ofEpochMilli(file.lastModified());
    }

    /**
     * Deletes the object
     */
    public void delete() {
        if (!file.delete()) {
            Storage.LOG.WARN("Failed to delete data file for object %s (%s).", getName(), file.getAbsolutePath());
        }
        if (!getPropertiesFile().delete()) {
            Storage.LOG.WARN("Failed to delete properties file for object %s (%s).",
                             getName(),
                             getPropertiesFile().getAbsolutePath());
        }
    }

    /**
     * Returns the underlying file
     *
     * @return the underlying file containing the stored contents
     */
    public File getFile() {
        return file;
    }

    /**
     * Determins if the object exists
     *
     * @return <tt>true</tt> if the object exists, <tt>false</tt> otherwise
     */
    public boolean exists() {
        return file.exists();
    }

    /**
     * Returns all properties stored along with the object.
     * <p>
     * This is the Content-MD5, Content-Type and any x-amz-meta- header.
     * </p>
     *
     * @return a set of name value pairs representing all properties stored for this object or an empty set if no
     * properties could be read.
     */
    public Set<Map.Entry<Object, Object>> getProperties() {
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(getPropertiesFile())) {
            props.load(in);
        } catch (IOException e) {
            Exceptions.ignore(e);
        }
        return props.entrySet();
    }

    /**
     * Returns the file used to store the properties and meta headers.
     *
     * @return the underlying file used to store the meta infos
     */
    public File getPropertiesFile() {
        return new File(file.getParentFile(), "__ninja_" + file.getName() + ".properties");
    }

    /**
     * Stores the given meta infos for the stored object.
     *
     * @param properties properties to store
     * @throws IOException in case of an IO error
     */
    public void storeProperties(Map<String, String> properties) throws IOException {
        Properties props = new Properties();
        properties.forEach(props::setProperty);
        try (FileOutputStream out = new FileOutputStream(getPropertiesFile())) {
            props.store(out, "");
        }
    }
}
