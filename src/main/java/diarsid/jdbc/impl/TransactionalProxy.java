package diarsid.jdbc.impl;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import diarsid.jdbc.api.Jdbc;
import diarsid.jdbc.api.TransactionAware;
import diarsid.jdbc.api.exceptions.ForbiddenTransactionOperation;
import diarsid.jdbc.impl.transaction.JdbcTransactionReal;
import diarsid.support.objects.CommonEnum;
import org.slf4j.LoggerFactory;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import static diarsid.jdbc.api.JdbcTransaction.State.OPEN;
import static diarsid.jdbc.api.JdbcTransaction.ThenDo.CLOSE;

public final class TransactionalProxy implements InvocationHandler, TransactionAware {

    enum TransactionJoining implements CommonEnum<TransactionJoining> {
        JOINED_TO_EXISTING,
        CREATED_NEW
    }

    private final Object transactional;

    private final TransactionAware transactionalAware;
    private final boolean isTransactionalAware;
    private final ThreadLocal<List<Throwable>> transactionalAwareExceptionsHolder;

    private final TransactionAware transactionalAware2;
    private final boolean hasTransactionalAware;
    private final ThreadLocal<List<Throwable>> transactionalAware2ExceptionsHolder;

    private final JdbcImpl jdbc;
    private final Jdbc.WhenNoTransactionThen whenNoTransactionThen;

    public TransactionalProxy(
            Object transactional,
            JdbcImpl jdbc,
            Jdbc.WhenNoTransactionThen whenNoTransactionThen) {
        this.transactional = transactional;
        this.transactionalAwareExceptionsHolder = new ThreadLocal<>();
        this.transactionalAware2ExceptionsHolder = new ThreadLocal<>();
        this.jdbc = jdbc;
        this.whenNoTransactionThen = whenNoTransactionThen;

        if ( transactional instanceof TransactionAware ) {
            this.isTransactionalAware = true;
            this.transactionalAware = (TransactionAware) this.transactional;
        }
        else {
            this.isTransactionalAware = false;
            this.transactionalAware = null;
        }

        this.hasTransactionalAware = false;
        this.transactionalAware2 = null;
    }

    public TransactionalProxy(
            Object transactional,
            TransactionAware aware,
            JdbcImpl jdbc,
            Jdbc.WhenNoTransactionThen whenNoTransactionThen) {
        this.transactional = transactional;
        this.transactionalAwareExceptionsHolder = new ThreadLocal<>();
        this.transactionalAware2ExceptionsHolder = new ThreadLocal<>();
        this.jdbc = jdbc;
        this.whenNoTransactionThen = whenNoTransactionThen;

        if ( transactional instanceof TransactionAware ) {
            this.isTransactionalAware = true;
            this.transactionalAware = (TransactionAware) this.transactional;
        }
        else {
            this.isTransactionalAware = false;
            this.transactionalAware = null;
        }

        this.hasTransactionalAware = true;
        this.transactionalAware2 = aware;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
        JdbcTransactionThreadBindingControl threadBinding = this.jdbc.threadBinding();

        JdbcTransactionReal currentTransaction;
        try {
            if ( threadBinding.isBound() ) {
                currentTransaction = (JdbcTransactionReal) threadBinding.currentTransaction();
                if ( currentTransaction.state().equalTo(OPEN) ) {
                    try {
                        this.beforeTransactionJoinFor(method, args);
                        Object result = method.invoke(this.transactional, args);
                        return result;
                    }
                    catch (InvocationTargetException i) {
                        if ( currentTransaction.state().equalTo(OPEN) ) {
                            currentTransaction.fail();
                        }
                        throw asUnchecked(i.getTargetException());
                    }
                    catch (RuntimeException e) {
                        if ( currentTransaction.state().equalTo(OPEN) ) {
                            currentTransaction.fail();
                        }
                        throw e;
                    }
                    catch (Throwable t) {
                        if ( currentTransaction.state().equalTo(OPEN) ) {
                            currentTransaction.fail();
                        }
                        throw asUnchecked(t);
                    }
                }
                else {
                    throw new ForbiddenTransactionOperation("Transaction is " + currentTransaction.state());
                }
            }
            else {
                this.beforeTransactionOpenFor(method, args);
                threadBinding.bindNew();
                this.afterTransactionOpenFor(method, args);
                currentTransaction = (JdbcTransactionReal) threadBinding.currentTransaction();
                try {
                    Object result = method.invoke(this.transactional, args);
                    this.beforeTransactionCommitAndCloseFor(method, args);
                    currentTransaction.commitAndClose();
                    this.afterTransactionCommitAndCloseFor(method, args);

                    return result;
                }
                catch (InvocationTargetException i) {
                    this.beforeTransactionRollbackAndCloseFor(method, args);
                    currentTransaction.rollbackAnd(CLOSE);
                    this.afterTransactionRollbackAndCloseFor(method, args);
                    throw asUnchecked(i.getTargetException());
                }
                catch (RuntimeException e) {
                    this.beforeTransactionRollbackAndCloseFor(method, args);
                    currentTransaction.rollbackAnd(CLOSE);
                    this.afterTransactionRollbackAndCloseFor(method, args);
                    throw e;
                }
                catch (Throwable t) {
                    this.beforeTransactionRollbackAndCloseFor(method, args);
                    currentTransaction.rollbackAnd(CLOSE);
                    this.afterTransactionRollbackAndCloseFor(method, args);
                    throw asUnchecked(t);
                }
                finally {
                    threadBinding.unbind();
                }
            }
        }
        finally {
            try {
                this.supplyHoldExceptionsIfAny();
            }
            finally {
                this.transactionalAwareExceptionsHolder.remove();
                this.transactionalAware2ExceptionsHolder.remove();
            }
        }
    }

    private static RuntimeException asUnchecked(Throwable t) {
        if ( t instanceof RuntimeException ) {
            return (RuntimeException) t;
        }
        else {
            Throwable cause = t.getCause();
            if ( nonNull(cause) ) {
                if ( cause instanceof RuntimeException ) {
                    return (RuntimeException) cause;
                }
                else {
                    return new RuntimeException(cause);
                }
            }
            else {
                return new RuntimeException(t);
            }
        }
    }

    private static void holdLocallyIn(Throwable t, ThreadLocal<List<Throwable>> threadLocal) {
        List<Throwable> throwables = threadLocal.get();

        if ( isNull(throwables) ) {
            throwables = new ArrayList<>();
            threadLocal.set(throwables);
        }

        throwables.add(t);
    }

    private void holdLocally(Throwable t) {
        holdLocallyIn(t, this.transactionalAwareExceptionsHolder);
    }

    private void holdLocally2(Throwable t) {
        holdLocallyIn(t, this.transactionalAware2ExceptionsHolder);
    }

    private void supplyHoldExceptionsIfAny() {
        if ( this.isTransactionalAware ) {
            List<Throwable> throwables = this.transactionalAwareExceptionsHolder.get();
            if ( nonNull(throwables) ) {
                try {
                    this.transactionalAware.onTransactionAwareOwnExceptions(throwables);
                }
                catch (Throwable t) {
                    logUnprocessed(t);
                }
            }
        }

        if ( this.hasTransactionalAware ) {
            List<Throwable> throwables = this.transactionalAware2ExceptionsHolder.get();
            if ( nonNull(throwables) ) {
                try {
                    this.transactionalAware2.onTransactionAwareOwnExceptions(throwables);
                }
                catch (Throwable t) {
                    logUnprocessed(t);
                }
            }
        }
    }

    private void logUnprocessed(Throwable t) {
        LoggerFactory.getLogger(this.getClass()).error("cannot report normally: ", t);
    }

    @Override
    public final void beforeTransactionOpenFor(Method method, Object[] args) {
        if ( this.isTransactionalAware) {
            try {
                this.transactionalAware.beforeTransactionOpenFor(method, args);
            }
            catch (Throwable t) {
                this.holdLocally(t);
            }
        }

        if ( this.hasTransactionalAware ) {
            try {
                this.transactionalAware2.beforeTransactionOpenFor(method, args);
            }
            catch (Throwable t) {
                this.holdLocally2(t);
            }
        }
    }

    @Override
    public final void afterTransactionOpenFor(Method method, Object[] args) {
        if ( this.isTransactionalAware) {
            try {
                this.transactionalAware.afterTransactionOpenFor(method, args);
            }
            catch (Throwable t) {
                this.holdLocally(t);
            }
        }

        if ( this.hasTransactionalAware ) {
            try {
                this.transactionalAware2.afterTransactionOpenFor(method, args);
            }
            catch (Throwable t) {
                this.holdLocally2(t);
            }
        }
    }

    @Override
    public final void beforeTransactionJoinFor(Method method, Object[] args) {
        if ( this.isTransactionalAware) {
            try {
                this.transactionalAware.beforeTransactionJoinFor(method, args);
            }
            catch (Throwable t) {
                this.holdLocally(t);
            }
        }

        if ( this.hasTransactionalAware ) {
            try {
                this.transactionalAware2.beforeTransactionJoinFor(method, args);
            }
            catch (Throwable t) {
                this.holdLocally2(t);
            }
        }
    }

    @Override
    public final void beforeTransactionCommitAndCloseFor(Method method, Object[] args) {
        if ( this.isTransactionalAware) {
            try {
                this.transactionalAware.beforeTransactionCommitAndCloseFor(method, args);
            }
            catch (Throwable t) {
                    this.holdLocally(t);
            }
        }

        if ( this.hasTransactionalAware ) {
            try {
                this.transactionalAware2.beforeTransactionCommitAndCloseFor(method, args);
            }
            catch (Throwable t) {
                this.holdLocally2(t);
            }
        }
    }

    @Override
    public final void beforeTransactionRollbackAndCloseFor(Method method, Object[] args) {
        if ( this.isTransactionalAware) {
            try {
                this.transactionalAware.beforeTransactionRollbackAndCloseFor(method, args);
            }
            catch (Throwable t) {
                this.holdLocally(t);
            }
        }

        if ( this.hasTransactionalAware ) {
            try {
                this.transactionalAware2.beforeTransactionRollbackAndCloseFor(method, args);
            }
            catch (Throwable t) {
                this.holdLocally2(t);
            }
        }
    }

    @Override
    public final void afterTransactionCommitAndCloseFor(Method method, Object[] args) {
        if ( this.isTransactionalAware) {
            try {
                this.transactionalAware.afterTransactionCommitAndCloseFor(method, args);
            }
            catch (Throwable t) {
                this.holdLocally(t);
            }
        }

        if ( this.hasTransactionalAware ) {
            try {
                this.transactionalAware2.afterTransactionCommitAndCloseFor(method, args);
            }
            catch (Throwable t) {
                this.holdLocally2(t);
            }
        }
    }

    @Override
    public final void afterTransactionRollbackAndCloseFor(Method method, Object[] args) {
        if ( this.isTransactionalAware) {
            try {
                this.transactionalAware.afterTransactionRollbackAndCloseFor(method, args);
            }
            catch (Throwable t) {
                this.holdLocally(t);
            }
        }

        if ( this.hasTransactionalAware ) {
            try {
                this.transactionalAware2.afterTransactionRollbackAndCloseFor(method, args);
            }
            catch (Throwable t) {
                this.holdLocally2(t);
            }
        }
    }
}
