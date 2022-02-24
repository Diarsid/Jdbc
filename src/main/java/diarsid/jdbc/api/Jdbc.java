package diarsid.jdbc.api;

import java.nio.file.Path;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import diarsid.jdbc.impl.JdbcBuilder;
import diarsid.support.functional.ThrowingConsumer;
import diarsid.support.functional.ThrowingFunction;
import diarsid.support.objects.CommonEnum;

public interface Jdbc extends JdbcOperations, AutoCloseable {

    enum WhenNoTransactionThen implements CommonEnum<WhenNoTransactionThen> {
        IF_NO_TRANSACTION_THROW,
        IF_NO_TRANSACTION_OPEN_NEW
    }

    static Jdbc init(SqlConnectionsSource source) {
        return new JdbcBuilder(source).build();
    }

    static Jdbc init(SqlConnectionsSource source, Map<JdbcOption, Object> options) {
        return new JdbcBuilder(source, options).build();
    }

    JdbcTransaction createTransaction();

    void doInTransaction(Consumer<ThreadBoundJdbcTransaction> transactionalOperation);

    <T> T doInTransaction(Function<ThreadBoundJdbcTransaction, T> transactionalFunction);

    void doInTransactionThrowing(ThrowingConsumer<ThreadBoundJdbcTransaction> transactionalOperation) throws Throwable;

    <T> T doInTransactionThrowing(ThrowingFunction<ThreadBoundJdbcTransaction, T> transactionalFunction) throws Throwable;

    <P> P createTransactionalProxyFor(Class<P> type, P p, WhenNoTransactionThen then);

    <P> P createTransactionalProxyFor(Class<P> type, P p, TransactionAware aware, WhenNoTransactionThen then);

    JdbcTransactionThreadBinding threadBinding();

    void change(JdbcOption option, Object value);

//    void executeScript(Path file);

    @Override
    void close();

}
