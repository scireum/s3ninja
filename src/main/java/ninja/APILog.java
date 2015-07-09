/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package ninja;

import com.google.common.collect.Lists;
import sirius.kernel.commons.Watch;
import sirius.kernel.di.std.Register;
import sirius.kernel.nls.NLS;

import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.List;

/**
 * Contains a log of the latest API calls
 * <p>
 * The entries are stored in memory and will be lost during server restarts. Also the site is limited to 250 entries.
 * The newest entry will be the first in the list.
 */
@Register(classes = APILog.class)
public class APILog {

    /**
     * Used to describe if a call was successful or why if failed.
     */
    public enum Result {
        OK, REJECTED, ERROR
    }

    /**
     * Represents a log entry.
     */
    public static class Entry {
        private String tod = NLS.toUserString(LocalDateTime.now());
        private String function;
        private String description;
        private String result;
        private String duration;

        /**
         * Returns the method or function which was called.
         *
         * @return the name of the called function
         */
        public String getFunction() {
            return function;
        }

        /**
         * Returns a description of the call
         *
         * @return a short text describing the call
         */
        public String getDescription() {
            return description;
        }

        /**
         * Returns the outcome (one of Result.name()) of the call
         *
         * @return a string describing if the call succeeded or why it failed.
         */
        public String getResult() {
            return result;
        }

        /**
         * Returns the duration of the call
         *
         * @return a textual representation of the duration of the call
         */
        public String getDuration() {
            return duration;
        }

        /**
         * Returns a timestamp of the call
         *
         * @return a string representation of the timestamp when the call was invoked.
         */
        public String getTod() {
            return tod;
        }

        /**
         * Helper method which returns a bootstrap css class based on the result of the call
         *
         * @return a css class used to represent the result of the call.
         */
        public String getCSS() {
            if ("ERROR".equals(result)) {
                return "error";
            }
            if ("REJECTED".equals(result)) {
                return "warning";
            }

            return "";
        }

        /**
         * Creates a new log entry.
         *
         * @param function    name or method of the function which was invoked
         * @param description description of the call
         * @param result      outcome of the call
         * @param duration    duration of the call
         */
        protected Entry(String function, String description, String result, String duration) {
            this.function = function;
            this.description = description;
            this.result = result;
            this.duration = duration;
        }
    }

    private final List<Entry> entries = Lists.newArrayList();

    /**
     * Returns a sublist of the stored entries, starting at <tt>start</tt> returning at most <tt>count</tt> items.
     *
     * @param start index of the item where to start
     * @param count max number of items returned
     * @return a non null list of log entries
     */
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

    /**
     * Creates a new log entry.
     *
     * @param function    name or method of the function which was invoked
     * @param description description of the call
     * @param result      outcome of the call
     * @param watch       watch representing the duration of the call
     */
    public void log(String function, String description, Result result, Watch watch) {
        synchronized (entries) {
            entries.add(0, new Entry(function, description, result.name(), watch.duration()));
            if (entries.size() > 250) {
                entries.remove(entries.size() - 1);
            }
        }
    }
}
