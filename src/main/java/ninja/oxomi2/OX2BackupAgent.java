package ninja.oxomi2;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import io.netty.handler.codec.http.HttpHeaders;
import ninja.Bucket;
import ninja.Storage;
import ninja.StoredObject;
import sirius.kernel.commons.Context;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.ConfigValue;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.xml.Outcall;
import sirius.kernel.xml.StructuredInput;
import sirius.kernel.xml.StructuredNode;
import sirius.kernel.xml.XMLStructuredInput;
import sirius.web.http.MimeHelper;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Register(classes = OX2BackupAgent.class)
public class OX2BackupAgent implements Runnable {

    @ConfigValue("backupAgent.baseUrl")
    public static String baseUrl;

    @ConfigValue("backupAgent.agentName")
    public static String agentName;

    @ConfigValue("backupAgent.key")
    public static String key;

    @Part
    public static Storage storage;

    private static AtomicInteger filesTransferred = new AtomicInteger(0);

    private static AtomicLong bytesTransferred = new AtomicLong(0l);

    private static boolean running = false;


    @Override
    public void run() {
        final String serviceUrl = baseUrl + "/service/xml/backup/get-backup-files";

        while (true) {
            try {
                if (!shouldRun() || !isRunning()) {
                    break;
                }

                // backup process: Query list of files to backup and get them.
                final StructuredInput in = callOxomiServiceForBackupFiles(serviceUrl);

                List<StructuredNode> files = in.getNode("files").queryNodeList("file");
                for (final StructuredNode n : files) {
                    final String category = n.queryString("category");
                    final String fileId = n.queryString("id");
                    final String filename = n.queryString("filename");
                    final Long fileSize = Value.of(n.queryString("size")).asLong(0);

                    // ensures the bucket exists.
                    Bucket bucket = storage.getBucket(category);
                    if (!bucket.exists()) {
                        bucket.create();
                    }

                    // get the file and put it into this bucket.
                    final InputStream is = downloadBackupFile(fileId);
                    storeFileToBucket(fileId, filename, bucket, is);

                    // increase statistics.
                    filesTransferred.incrementAndGet();
                    bytesTransferred.addAndGet(fileSize);

                    // break if backup was disabled.
                    if (!isRunning()) {
                        break;
                    }
                }

                // if there haven't been any files for backup, wait for a while.
                if (files.isEmpty()) {
                    Thread.sleep(30000);
                }
            } catch (Exception e) {
                Exceptions.handle(e);
            }
        }
    }

    /**
     * Call the oxomi2 service to get file information for backup
     *
     * @param serviceUrl the service to be called
     * @return the output from the service call.
     * @throws IOException on error
     */
    private StructuredInput callOxomiServiceForBackupFiles(String serviceUrl) throws IOException {
        final Outcall outcall = new Outcall(new URL(serviceUrl), signRequest());
        return new XMLStructuredInput(outcall.getInput(), true);
    }

    /**
     * @return true when backup should go on.
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * @param running true to start the backup agent, false to stop it
     */
    public void setRunning(final boolean running) {
        OX2BackupAgent.running = running;
    }

    /**
     * @return number of bytes transferred since application start.
     */
    public long getBytesTransferred() {
        return bytesTransferred.get();
    }

    /**
     * @return number of files transferred since application start.
     */
    public int getFilesTransferred() {
        return filesTransferred.get();
    }

    /**
     * @return true if the backup options are configured
     */
    private boolean shouldRun() {
        return Strings.isFilled(baseUrl) && Strings.isFilled(agentName) && Strings.isFilled(key);
    }


    private static InputStream downloadBackupFile(String fileId) throws IOException {
        return new Outcall(new URL(baseUrl + "/backup-agent/" + fileId), signRequest()).getInput();
    }

    private static void storeFileToBucket(String fileId, String filename, Bucket bucket, InputStream is) throws IOException {
        StoredObject object = bucket.getObject(fileId);
        try {
            try (FileOutputStream out = new FileOutputStream(object.getFile())) {
                ByteStreams.copy(is, out);
            }
        } finally {
            is.close();
        }

        Map<String, String> properties = Maps.newTreeMap();
        properties.put(HttpHeaders.Names.CONTENT_TYPE, MimeHelper.guessMimeType(filename));
        HashCode hash = Files.hash(object.getFile(), Hashing.md5());
        String md5 = BaseEncoding.base64().encode(hash.asBytes());
        properties.put("Content-MD5", md5);
        object.storeProperties(properties);
    }

    private static Context signRequest() {
        final Context context = new Context();
        context.put("agentName", agentName);

        final String signatureString = LocalDateTime.now().withHour(0).withMinute(0).withSecond(0).withNano(0).format(DateTimeFormatter.ISO_DATE) + "@" + agentName + "@" + key;
        context.put("key", Hashing.md5().hashString(signatureString, Charsets.UTF_8).toString());

        return context;
    }
}
