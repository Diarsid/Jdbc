package diarsid.jdbc.api;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import diarsid.jdbc.api.exceptions.JdbcException;
import diarsid.jdbc.impl.JdbcPreparedStatementSetter;
import diarsid.support.objects.CommonEnum;

import static java.lang.String.format;

public enum JdbcOption implements CommonEnum<JdbcOption> {

    JDBC_PREPARED_STATEMENT_SETTERS(
            false,
            JdbcPreparedStatementSetter[].class,
            Collection.class,
            List.class,
            Set.class),

    TRANSACTION_GUARD_ENABLED(
            false,
            boolean.class,
            Boolean.class),

    TRANSACTION_GUARD_DELAY_IN_MS(
            false,
            Integer.class,
            Long.class),

    TRANSACTION_GUARD_SCHEDULER(
            false,
            ScheduledExecutorService.class),

    SQL_HISTORY_ENABLED(
            true,
            boolean.class,
            Boolean.class),

    SQL_HISTORY_PARAMS_REPLACE(
            true,
            boolean.class,
            Boolean.class);

    private final boolean changeable;
    private final Class[] classes;

    JdbcOption(boolean changeable, Class... acceptableParams) {
        this.changeable = changeable;
        this.classes = acceptableParams;
    }

    public boolean isChangeable() {
        return this.changeable;
    }

    public boolean isNotChangeable() {
        return ! this.changeable;
    }

    public void mustSupportClassOf(Object value) {
        for ( Class type : classes ) {
            if ( value.getClass().isAssignableFrom(type) ) {
                return;
            }
        }

        throw new JdbcException(format("Value of class %s is not supported by %s %s",
                value.getClass().getSimpleName(),
                JdbcOption.class.getSimpleName(),
                this.name()));
    }
}
