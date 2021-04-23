package diarsid.jdbc.impl.sqlhistory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

import diarsid.jdbc.api.SqlHistory;
import diarsid.support.objects.PooledReusable;

import static java.util.Arrays.asList;
import static java.util.Objects.nonNull;

import static diarsid.jdbc.api.SqlHistory.Query.ArgsType.ARRAY;
import static diarsid.jdbc.api.SqlHistory.Query.ArgsType.LIST;
import static diarsid.jdbc.api.SqlHistory.Query.ArgsType.NONE;
import static diarsid.jdbc.api.SqlHistory.Record.Type.COMMENT;
import static diarsid.jdbc.api.SqlHistory.Record.Type.EXCEPTION;
import static diarsid.jdbc.api.SqlHistory.Record.Type.QUERY;
import static diarsid.jdbc.api.SqlHistory.Record.Type.ROLLBACK;

public class SqlHistoryRecorder extends PooledReusable implements SqlHistoryRecording, SqlHistory {

    static class SqlHistoryQuery implements Query {

        private final int index;
        private final String query;
        private final Long millis;
        private final ArgsType argsType;
        private final List argsAsList;
        private final Object[] argsAsArray;
        private final Boolean isBatch;

        public SqlHistoryQuery(
                int index, String query, Long millis) {
            this.index = index;
            this.query = query;
            this.millis = millis;
            this.argsType = NONE;
            this.argsAsList = null;
            this.argsAsArray = null;
            this.isBatch = false;
        }

        public SqlHistoryQuery(
                int index, String query, Long millis, List argsAsList, Boolean isBatch) {
            this.index = index;
            this.query = query;
            this.millis = millis;
            this.argsType = LIST;
            this.argsAsList = new ArrayList(argsAsList);
            this.argsAsArray = null;
            this.isBatch = isBatch;
        }

        public SqlHistoryQuery(
                int index, String query, Long millis, Object[] argsAsArray, Boolean isBatch) {
            this.index = index;
            this.query = query;
            this.millis = millis;
            this.argsType = ARRAY;
            this.argsAsList = null;
            this.argsAsArray = Arrays.copyOf(argsAsArray, argsAsArray.length);
            this.isBatch = isBatch;
        }

        @Override
        public String string() {
            return query;
        }

        @Override
        public int index() {
            return index;
        }

        @Override
        public Long millis() {
            return millis;
        }

        @Override
        public boolean isBatch() {
            return isBatch;
        }

        @Override
        public ArgsType argsType() {
            return argsType;
        }

        @Override
        public Object[] argsAsArray() {
            if ( nonNull(argsAsArray) ) {
                return argsAsArray;
            }

            return argsAsList.toArray();
        }

        @Override
        public List argsAsList() {
            if ( nonNull(argsAsList) ) {
                return argsAsList;
            }

            return asList(argsAsArray);
        }

        @Override
        public SqlHistory.Record.Type type() {
            return QUERY;
        }
    }

    static class SqlHistoryRollback implements Record {

        private final int index;
        private final Long millis;

        public SqlHistoryRollback(int index, Long millis) {
            this.index = index;
            this.millis = millis;
        }

        @Override
        public String string() {
            return "ROLLBACK";
        }

        @Override
        public int index() {
            return this.index;
        }

        @Override
        public Long millis() {
            return this.millis;
        }

        @Override
        public SqlHistory.Record.Type type() {
            return ROLLBACK;
        }
    }

    static class SqlHistoryComment implements Record {

        private final int index;
        private final String comment;

        public SqlHistoryComment(int index, String comment) {
            this.index = index;
            this.comment = comment;
        }

        @Override
        public String string() {
            return comment;
        }

        @Override
        public int index() {
            return index;
        }

        @Override
        public Long millis() {
            return 0L;
        }

        @Override
        public SqlHistory.Record.Type type() {
            return COMMENT;
        }
    }

    static class SqlHistoryException implements Record {

        private final int index;
        private final Exception exception;

        public SqlHistoryException(int index, Exception exception) {
            this.index = index;
            this.exception = exception;
        }

        public Exception exception() {
            return exception;
        }

        @Override
        public String string() {
            return exception.toString();
        }

        @Override
        public int index() {
            return index;
        }

        @Override
        public Long millis() {
            return 0L;
        }

        @Override
        public Type type() {
            return EXCEPTION;
        }
    }

    private static final int UNREPORTED = -1;

    private final List<Record> records;
    private final boolean replaceParamsInSqlHistory;
    private final UUID transactionUuid;
    private int lastReportedRecordIndex;

    public SqlHistoryRecorder(UUID transactionUuid, boolean replaceParamsInSqlHistory) {
        this.transactionUuid = transactionUuid;
        this.replaceParamsInSqlHistory = replaceParamsInSqlHistory;
        this.records = new ArrayList<>();
        this.lastReportedRecordIndex = UNREPORTED;
    }

    @Override
    public void add(String message) {
        Record comment = new SqlHistoryComment(this.records.size(), message);
        this.records.add(comment);
    }

    @Override
    public void add(String sql, long millis) {
        Query query = new SqlHistoryQuery(this.records.size(), sql, millis);
        this.records.add(query);
    }

    @Override
    public void add(String sql, List args, long millis) {
        Query query = new SqlHistoryQuery(this.records.size(), sql, millis, args, false);
        this.records.add(query);
    }

    @Override
    public void add(String sql, Object[] args, long millis) {
        Query query = new SqlHistoryQuery(this.records.size(), sql, millis, args, false);
        this.records.add(query);
    }

    @Override
    public void addBatch(String sql, List<List> args, long millis) {
        Query query = new SqlHistoryQuery(this.records.size(), sql, millis, args, true);
        this.records.add(query);
    }

    @Override
    public void add(Exception e) {
        SqlHistoryException exception = new SqlHistoryException(this.records.size(), e);
        this.records.add(exception);
    }

    @Override
    public void addRollback(long millis) {
        Record rollback = new SqlHistoryRollback(this.records.size(), millis);
        this.records.add(rollback);
    }

    @Override
    public int recordsTotal() {
        return this.records.size();
    }

    @Override
    public Record recordOf(int index) {
        return this.records.get(index);
    }

    @Override
    public String reportAll() {
        return this.reportRecordsFrom(UNREPORTED);
    }

    @Override
    public String reportLast() {
        return this.reportRecordsFrom(this.lastReportedRecordIndex);
    }

    public String reportRecordsFrom(int fromIndexExcl) {
        int fromIndex = fromIndexExcl + 1;

        if ( fromIndex >= this.records.size() ) {
            return "NO UNREPORTED RECORDS";
        }

        SqlHistoryReportMaker reportMaker = new SqlHistoryReportMaker(
                this.replaceParamsInSqlHistory, this.transactionUuid);

        SqlHistory.Record.Type type;
        SqlHistory.Query query;
        Record record;
        for ( int i = fromIndex; i < this.records.size(); i++ ) {
            record = this.records.get(i);
            type = record.type();

            switch ( type ) {
                case QUERY:
                    query = (Query) record;
                    switch ( query.argsType() ) {
                        case LIST:
                            reportMaker.add(query.string(), query.millis(), query.isBatch(), query.argsAsList());
                            break;
                        case ARRAY:
                            reportMaker.add(query.string(), query.millis(), query.isBatch(), query.argsAsArray());
                            break;
                        case NONE:
                            reportMaker.add(query.string(), query.millis());
                            break;
                        default:
                            throw query.argsType().unsupported();
                    }
                    break;
                case ROLLBACK:
                case COMMENT:
                    reportMaker.addComment(record.string(), record.millis());
                    break;
                case EXCEPTION:
                    reportMaker.add(((SqlHistoryException) record).exception());
                    break;
                default:
                    throw type.unsupported();
            }
        }

        String report = reportMaker.makeReport();
        reportMaker.clear();

        this.lastReportedRecordIndex = this.records.size() - 1;

        return report;
    }

    @Override
    public void reported() {
        this.lastReportedRecordIndex = this.records.size() - 1;
    }

    @Override
    public boolean hasUnreported() {
        return (this.records.size() - 1) > this.lastReportedRecordIndex;
    }

    @Override
    public long millisTotal() {
        return 0;
    }

    @Override
    public void forEach(Consumer<Record> recordViewer) {
        for ( Record record : this.records ) {
            try {
                recordViewer.accept(record);
            }
            catch (Throwable t) {
                // TODO
            }
        }
    }

    @Override
    protected void clearForReuse() {
        this.records.clear();
    }
}
