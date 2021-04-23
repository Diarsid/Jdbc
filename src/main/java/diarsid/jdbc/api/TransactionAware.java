package diarsid.jdbc.api;

import java.lang.reflect.Method;

public interface TransactionAware {

    default void beforeTransactionOpenFor(Method method, Object[] args) {}

    default void afterTransactionOpenFor(Method method, Object[] args) {}

    default void beforeTransactionJoinFor(Method method, Object[] args) {}

    default void beforeTransactionCommitAndCloseFor(Method method, Object[] args) {}

    default void beforeTransactionRollbackAndCloseFor(Method method, Object[] args) {}

    default void afterTransactionCommitAndCloseFor(Method method, Object[] args) {}

    default void afterTransactionRollbackAndCloseFor(Method method, Object[] args) {}
}
