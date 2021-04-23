package diarsid.jdbc.impl.sqlhistory;

import java.util.List;

public interface SqlHistoryRecording {

    void add(String message);

    void add(String sql, long millis);

    void add(String sql, List args, long millis);

    void add(String sql, Object[] args, long millis);

    void addBatch(String sql, List<List> args, long millis);

    void add(Exception e);

    void addRollback(long millis);
}
