package diarsid.jdbc.api.exceptions;

public class ForbiddenTransactionOperation extends JdbcException {

    public ForbiddenTransactionOperation(String msg) {
        super(msg);
    }
}
