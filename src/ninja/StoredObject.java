package ninja;

import org.joda.time.DateTime;
import sirius.kernel.nls.NLS;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Andreas Haufler (aha@scireum.de)
 * @since 2013/08
 */
public class StoredObject {
    private File file;

    public StoredObject(File file) {
        this.file = file;
    }

    public String getName() {
        return file.getName();
    }

    public String getSize() {
        return NLS.formatSize(file.length());
    }

    public String getLastModified() {
        return NLS.toUserString(new DateTime(file.lastModified()), true);
    }

    public void delete() {
        file.delete();
        getPropertiesFile().delete();
    }

    public File getFile() {
        return file;
    }

    public boolean exists() {
        return file.exists();
    }

    public Set<Map.Entry<Object, Object>> getProperties() throws Exception {
        Properties props = new Properties();
        FileInputStream in = new FileInputStream(getPropertiesFile());
        try {
            props.load(in);
        } finally {
            in.close();
        }

        return props.entrySet();
    }

    public File getPropertiesFile() {
        return new File(file.getParentFile(), "__ninja_" + file.getName() + ".properties");
    }

    public void storeProperties(Map<String, String> properties) throws IOException {
        Properties props = new Properties();
        props.putAll(properties);
        FileOutputStream out = new FileOutputStream(getPropertiesFile());
        try {
            props.store(out, "");
        } finally {
            out.close();
        }
    }
}
