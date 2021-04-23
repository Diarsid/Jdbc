package diarsid.jdbc.api;

public interface JdbcTransactionThreadBinding {

    ThreadBoundJdbcTransaction currentTransaction();

    boolean isBound();

    default boolean isNotBound() {
        return ! this.isBound();
    }
}
