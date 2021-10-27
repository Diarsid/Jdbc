package diarsid.jdbc.api;

import java.lang.reflect.Method;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

public interface TransactionAware {

    default void beforeTransactionOpenFor(Method method, Object[] args) {}

    default void afterTransactionOpenFor(Method method, Object[] args) {}

    default void beforeTransactionJoinFor(Method method, Object[] args) {}

    default void beforeTransactionCommitAndCloseFor(Method method, Object[] args) {}

    default void beforeTransactionRollbackAndCloseFor(Method method, Object[] args) {}

    default void afterTransactionCommitAndCloseFor(Method method, Object[] args) {}

    default void afterTransactionRollbackAndCloseFor(Method method, Object[] args) {}

    default void onTransactionAwareOwnExceptions(List<Throwable> ts) {
        Logger log = LoggerFactory.getLogger(this.getClass());
        for ( Throwable t : ts ) {
            log.error(format("Exception occurred on %s %s method: %s",
                    TransactionAware.class.getSimpleName(),
                    this.getClass().getSimpleName(),
                    t.getMessage()), t);
        }
    }
}
