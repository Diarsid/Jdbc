package diarsid.jdbc.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diarsid.jdbc.api.Jdbc;
import diarsid.jdbc.api.JdbcTransaction;
import diarsid.jdbc.api.ThreadBoundJdbcTransaction;
import diarsid.jdbc.api.exceptions.ForbiddenTransactionOperation;
import diarsid.jdbc.api.exceptions.JdbcException;
import diarsid.jdbc.impl.transaction.JdbcTransactionReal;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import static diarsid.jdbc.api.JdbcTransaction.State.CLOSED_COMMITTED;
import static diarsid.jdbc.api.JdbcTransaction.State.CLOSED_ROLLBACKED;
import static diarsid.jdbc.api.JdbcTransaction.State.FAILED;
import static diarsid.jdbc.api.JdbcTransaction.State.OPEN;
import static diarsid.jdbc.api.JdbcTransaction.ThenDo.CLOSE;

public class JdbcTransactionThreadBindingControlImpl implements JdbcTransactionThreadBindingControl {

    private static final Logger log = LoggerFactory.getLogger(JdbcTransactionThreadBindingControlImpl.class);

    private final ThreadLocal<JdbcTransactionReal> threadJdbcTransactions;
    private final Jdbc jdbc;

    public JdbcTransactionThreadBindingControlImpl(Jdbc jdbc) {
        this.jdbc = jdbc;
        this.threadJdbcTransactions = new ThreadLocal<>();
    }

//    private TransactionalProxy.TransactionJoining joinExistingTransactionOr(Jdbc.WhenNoTransactionThen then) {
//        JdbcTransactionReal transaction = threadJdbcTransactions.get();
//        TransactionalProxy.TransactionJoining joining;
//
//        if ( isNull(transaction) ) {
//            switch ( then ) {
//                case IF_NO_TRANSACTION_THROW:
//                    throw new JdbcException("There is no open transaction!");
//                case IF_NO_TRANSACTION_OPEN_NEW:
//                    transaction = (JdbcTransactionReal) jdbc.createTransaction();
//                    threadJdbcTransactions.set(transaction);
//                    joining = CREATED_NEW;
//                    break;
//                default:
//                    throw new UnsupportedOperationException();
//            }
//        }
//        else {
//            joining = JOINED_TO_EXISTING;
//        }
//
//        return joining;
//    }

    @Override
    public ThreadBoundJdbcTransaction currentTransaction() {
        ThreadBoundJdbcTransaction transaction = threadJdbcTransactions.get();

        if ( isNull(transaction) ) {
            throw new JdbcException("There is no open transaction!");
        }

        return transaction;
    }

    @Override
    public void unbind() {
        threadJdbcTransactions.remove();
    }

    @Override
    public void bindNew() {
        JdbcTransactionReal transaction = (JdbcTransactionReal) jdbc.createTransaction();
        threadJdbcTransactions.set(transaction);
    }

    @Override
    public void bindExisting(JdbcTransaction transaction) {
        threadJdbcTransactions.set((JdbcTransactionReal) transaction);
    }

    @Override
    public boolean isBound() {
        return nonNull(threadJdbcTransactions.get());
    }

    public void commitCurrentTransaction() {
        JdbcTransaction transaction = threadJdbcTransactions.get();

        if ( isNull(transaction) ) {
            throw new JdbcException("Nothing to commit - there is no open transaction!");
        }

        threadJdbcTransactions.remove();

        if ( transaction.state().equalTo(OPEN) ) {
            transaction.commitAndClose();
        }
        else if ( transaction.state().equalTo(FAILED) ) {
            transaction.rollbackAnd(CLOSE);
        }
        else if ( transaction.state().equalToAny(CLOSED_ROLLBACKED, CLOSED_COMMITTED) ) {
            throw new ForbiddenTransactionOperation("Transaction is already closed and cannot be committed again");
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    public void rollbackCurrentTransaction() {
        JdbcTransaction transaction = threadJdbcTransactions.get();

        if ( isNull(transaction) ) {
            throw new JdbcException("Nothing to rollback - there is no open transaction!");
        }

        threadJdbcTransactions.remove();

        if ( transaction.state().equalToAny(OPEN, FAILED) ) {
            transaction.rollbackAnd(CLOSE);
        }
        else if ( transaction.state().equalToAny(CLOSED_ROLLBACKED, CLOSED_COMMITTED) ) {
            throw new ForbiddenTransactionOperation("Transaction is already closed and cannot be rolled back again");
        }
        else {
            throw new UnsupportedOperationException();
        }
    }
}
