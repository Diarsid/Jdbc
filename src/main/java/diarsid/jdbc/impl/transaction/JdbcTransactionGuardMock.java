package diarsid.jdbc.impl.transaction;

import java.util.concurrent.TimeUnit;

public class JdbcTransactionGuardMock implements JdbcTransactionGuard {

    @Override
    public Runnable accept(JdbcTransactionReal transaction) {
        return null;
    }

    @Override
    public Runnable accept(JdbcTransactionReal transaction, int timeout, TimeUnit unit) {
        return null;
    }

    @Override
    public void stop() {
        // nothing to do
    }
    
}
