package diarsid.jdbc.api;

public interface ThreadBoundJdbcTransaction extends JdbcOperationsTransactional {

    void rollbackAndProceed();
}
