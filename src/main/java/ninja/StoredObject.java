/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.collect.Maps;
import sirius.kernel.commons.Files;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Urls;
import sirius.kernel.health.Exceptions;
import sirius.kernel.nls.NLS;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Represents a stored object.
 */
public class StoredObject {

    private static final String DELETED_MARKER = "DeletedMarker";

    /**
     * Contains the maximal length of an encoded key, which is what the 255 bytes of a file name leave once the name
     * of the properties file, <tt>$[key].properties</tt>, has been built from it.
     */
    private static final int MAX_ENCODED_KEY_LENGTH = 255 - "$.properties".length();

    private final File file;

    private final String key;

    private final String encodedKey;

    /**
     * Creates a new object from the given file.
     *
     * @param file the contents of the object.
     */
    public StoredObject(File file) {
        this.file = file;
        this.encodedKey = file.getName();
        this.key = decodeKey(this.encodedKey);

        if (!Strings.areEqual(this.encodedKey, encodeKey(this.key))) {
            throw Exceptions.createHandled()
                            .withSystemErrorMessage("File name \"%s\" is not properly encoded.", key)
                            .handle();
        }
    }

    /**
     * Creates a new object within the given bucket folder and the given key.
     *
     * @param folder the bucket's folder
     * @param key    the object's key
     */
    public StoredObject(File folder, String key) {
        this(new File(folder, encodeKey(key)));
    }

    /**
     * Encodes an object key for use as file name.
     *
     * @param key the key to encode
     * @return the encoded key
     */
    public static String encodeKey(String key) {
        return Urls.encode(key).replace("+", "%20");
    }

    /**
     * Decodes an encoded object key.
     *
     * @param key the key to decode
     * @return the decoded key
     */
    public static String decodeKey(String key) {
        try {
            return URLDecoder.decode(key, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw Exceptions.handle(Storage.LOG, e);
        }
    }

    /**
     * Returns the name of the object.
     *
     * @return the name of the object
     */
    public String getKey() {
        return key;
    }

    /**
     * Returns the encoded name of the object.
     *
     * @return the encoded name of the object
     */
    public String getEncodedKey() {
        return encodedKey;
    }

    /**
     * Returns the size of the object.
     *
     * @return a string representation of the byte-size of the object
     */
    public String getSize() {
        return NLS.formatSize(getSizeBytes());
    }

    /**
     * Returns the size of the object in bytes.
     *
     * @return the byte-size of the object
     */
    public long getSizeBytes() {
        return file.length();
    }

    /**
     * Returns the object's date of last modification.
     *
     * @return a string representation of the last modification date
     */
    public String getLastModified() {
        return NLS.toUserString(getLastModifiedInstant());
    }

    /**
     * Returns the object's date of last modification.
     *
     * @return the last modification date as {@link Instant}
     */
    public Instant getLastModifiedInstant() {
        return Instant.ofEpochMilli(file.lastModified());
    }

    /**
     * Deletes the object.
     */
    public void delete() {
        Files.delete(file);
        Files.delete(getPropertiesFile());
    }

    /**
     * Returns the underlying file.
     *
     * @return the underlying file containing the stored contents
     */
    public File getFile() {
        return file;
    }

    /**
     * Determines if the object exists.
     *
     * @return <b>true</b> if the object exists, <b>false</b> else
     */
    public boolean exists() {
        return file.exists();
    }

    /**
     * Returns the file used to store the properties and meta headers.
     *
     * @return the underlying file used to store the meta infos
     */
    public File getPropertiesFile() {
        return new File(file.getParentFile(), "$" + file.getName() + ".properties");
    }

    /**
     * Returns all meta information stored along with the object.
     * <p>
     * These are the <tt>Cache-Control</tt>, <tt>Content-Disposition</tt>, <tt>Content-MD5</tt>, <tt>Content-Type</tt>,
     * <tt>Expires</tt> and any <tt>x-amz-*</tt> headers.
     * <p>
     * Internally, a {@link Properties} file is loaded from disk and converted to a {@link Map}.
     *
     * @return name-value-pairs representing all properties stored for this object, or an empty map if no properties
     * could be read
     */
    public Map<String, String> getProperties() {
        // read properties object from disk
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(getPropertiesFile())) {
            props.load(in);
        } catch (IOException e) {
            Exceptions.ignore(e);
        }

        // convert the properties object to a string-to-string-map
        Map<String, String> map = Maps.newTreeMap();
        props.forEach((propertyKey, value) -> map.put(String.valueOf(propertyKey), String.valueOf(value)));
        return map;
    }

    /**
     * Returns a sorted list of all property names.
     *
     * @return a list of property names
     * @see #getProperties()
     */
    public List<String> getPropertyNames() {
        return getProperties().keySet().stream().sorted(String::compareToIgnoreCase).toList();
    }

    /**
     * Stores the given meta information for this object.
     * <p>
     * Internally, the map is transformed to a {@link Properties} object and stored to disk.
     *
     * @param properties the properties to store
     * @throws IOException in case of an IO error
     */
    public void setProperties(Map<String, String> properties) throws IOException {
        Properties props = new Properties();
        properties.forEach(props::setProperty);
        try (FileOutputStream out = new FileOutputStream(getPropertiesFile())) {
            props.store(out, "");
        }
    }

    /**
     * Makes the given file and the given properties the new state of this object.
     * <p>
     * The content appears in one step, so that readers never see a partly written object. The properties are written
     * before that: a reader which finds an object without an entity tag computes and stores one, which, the other way
     * round, could replace the properties of an object which has just been written. Content and properties are still
     * two files, so a reader may briefly see the previous content along with the new properties.
     *
     * @param content    the file holding the new content, which has to reside in the folder of the bucket
     * @param properties the properties of the new object
     * @throws IOException if the object cannot be written, in which case its previous state is kept
     */
    public void replaceWith(File content, Map<String, String> properties) throws IOException {
        Map<String, String> previousProperties = getPropertiesFile().exists() ? getProperties() : null;
        try {
            // Guarded as well, as a write which fails halfway leaves a damaged properties file behind.
            setProperties(properties);
            java.nio.file.Files.move(content.toPath(),
                                     file.toPath(),
                                     StandardCopyOption.ATOMIC_MOVE,
                                     StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException exception) {
            if (previousProperties != null) {
                setProperties(previousProperties);
            } else {
                Files.delete(getPropertiesFile());
            }
            throw exception;
        }
    }

    /**
     * Checks if the marker for "deleted" is set.
     * When an object is marked as "deleted" it can not be requested anymore.
     *
     * @return true if this file is "deleted"
     */
    public boolean isMarkedDeleted() {
        return getProperties().containsKey(DELETED_MARKER);
    }

    /**
     * Sets the object as "deleted", all requests onto this object are handled as if it is deleted.
     * <br><b>This method does not perform an actual delete!<br>To perform an actual delete please check {@link StoredObject#delete} </b>
     *
     * @throws IOException if the properties could not be updated
     */
    public void markDeleted() throws IOException {
        Map<String, String> fileProperties = getProperties();
        fileProperties.put(DELETED_MARKER, "true");
        setProperties(fileProperties);
    }

    /**
     * Checks whether an object with the given key can be stored.
     * <p>
     * An object is kept as a file named by its encoded key, next to a file for its properties. S3 permits keys of up
     * to 1024 bytes, which is more than a file name can hold, so longer keys have to be refused.
     *
     * @param key the key to check
     * @return <b>true</b> if the key is short enough to be stored, <b>false</b> else
     */
    public static boolean isStorableKey(String key) {
        return encodeKey(key).length() <= MAX_ENCODED_KEY_LENGTH;
    }

    /**
     * Checks whether the given string is valid for use as object key.
     * <p>
     * Currently, the key only must not be empty. All UTF-8 characters are valid, but names should be restricted to a
     * subset. See the <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html">official naming
     * recommendations</a>.
     *
     * @param key the key to check
     * @return <b>true</b> if the key is valid as object key, <b>false</b> else
     */
    public static boolean isValidKey(@Nullable String key) {
        return Strings.isFilled(key);
    }
}
