package diarsid.jdbc.api;

import java.time.LocalDateTime;
import java.util.UUID;

public interface JdbcOperationsTransactional extends JdbcOperations {

    UUID uuid();

    LocalDateTime created();

    SqlHistory sqlHistory();

    JdbcTransaction.State state();

    void doNotGuard();
}
