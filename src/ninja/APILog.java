package ninja;

import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import sirius.kernel.commons.Watch;
import sirius.kernel.di.std.Register;
import sirius.kernel.nls.NLS;

import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Andreas Haufler (aha@scireum.de)
 * @since 2013/08
 */
@Register(classes = APILog.class)
public class APILog {

    public static enum Result {
        OK, REJECTED, ERROR;
    }

    public static class Entry {
        private String tod = NLS.toUserString(new DateTime(), true);
        private String function;
        private String description;
        private String result;
        private String duration;
        private String css;

        public String getFunction() {
            return function;
        }

        public String getDescription() {
            return description;
        }

        public String getResult() {
            return result;
        }

        public String getDuration() {
            return duration;
        }

        public String getTod() {
            return tod;
        }

        public String getCSS() {
            if ("ERROR".equals(result)) {
                return "error";
            }
            if ("REJECTED".equals(result)) {
                return "warning";
            }

            return "";
        }

        public Entry(String function, String description, String result, String duration) {
            this.function = function;
            this.description = description;
            this.result = result;
            this.duration = duration;
        }
    }

    private List<Entry> entries = Lists.newArrayList();

    public List<Entry> getEntries(int start, int count) {
        List<Entry> result = Lists.newArrayList();
        synchronized (entries) {
            Iterator<Entry> iter = entries.iterator();
            while (iter.hasNext() && start > 0) {
                iter.next();
                start--;
            }
            while (iter.hasNext() && count > 0) {
                result.add(iter.next());
                count--;
            }
        }

        return result;
    }

    public void log(String function, String description, Result result, Watch watch) {
        synchronized (entries) {
            entries.add(0,new Entry(function, description, result.name(), watch.duration()));
            if (entries.size() > 250) {
                entries.remove(entries.size() - 1);
            }
        }
    }


}
