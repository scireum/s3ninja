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
 * Created with IntelliJ IDEA.
 *
 * @author Andreas Haufler (aha@scireum.de)
 * @since 2013/08
 */
public class Bucket {

    private File file;
    private static Cache<String, Boolean> publicAccessCache = CacheManager.createCache("public-bucket-access");


    public Bucket(File file) {
        this.file = file;
    }

    public String getName() {
        return file.getName();
    }

    public void delete() {
        for (File child : file.listFiles()) {
            child.delete();
        }
        file.delete();
    }

    public void create() {
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    public List<StoredObject> getObjects() {
        List<StoredObject> result = Lists.newArrayList();
        for (File child : file.listFiles()) {
            if (child.isFile() && !child.getName().startsWith("__")) {
                result.add(new StoredObject(child));
            }
        }

        return result;
    }

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

    public void makePrivate() {
        if (getPublicMarkerFile().exists()) {
            getPublicMarkerFile().delete();
            publicAccessCache.put(getName(), false);
        }
    }

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

    public File getFile() {
        return file;
    }

    public boolean exists() {
        return file.exists();
    }

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
