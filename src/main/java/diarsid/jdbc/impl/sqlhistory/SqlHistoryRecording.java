package diarsid.jdbc.impl.sqlhistory;

import java.util.List;

public interface SqlHistoryRecording {

    void add(String message);

    void add(List<String> messageLines);

    void add(String sql, long millis);

    void add(String sql, List args, long millis);

    void add(String sql, Object[] args, long millis);

    void add(String sql, long millis, List<String> messageLines);

    void add(String sql, List args, long millis, List<String> messageLines);

    void add(String sql, Object[] args, long millis, List<String> messageLines);

    void addBatch(String sql, List<List> args, long millis);

    void addBatchMappable(String sql, List<? extends Object> objects, long millis);

    void add(Throwable e);

    void addRollback(long millis);
}
