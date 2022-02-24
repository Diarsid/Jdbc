package diarsid.jdbc.impl;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import diarsid.jdbc.api.Jdbc;
import diarsid.jdbc.api.JdbcOption;
import diarsid.jdbc.api.SqlConnectionsSource;
import diarsid.jdbc.api.exceptions.JdbcException;
import diarsid.jdbc.impl.conversion.sql2java.SqlDoubleToJavaFloatConversion;
import diarsid.jdbc.impl.conversion.sql2java.SqlTimestampToSqlLocalDateTimeConversion;
import diarsid.jdbc.impl.conversion.sql2java.SqlTypeToJavaTypeConverter;
import diarsid.jdbc.impl.transaction.JdbcTransactionGuard;
import diarsid.jdbc.impl.transaction.JdbcTransactionGuardMock;
import diarsid.jdbc.impl.transaction.JdbcTransactionGuardReal;
import diarsid.support.objects.references.References;
import org.slf4j.LoggerFactory;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import static diarsid.jdbc.api.JdbcOption.JDBC_PREPARED_STATEMENT_SETTERS;
import static diarsid.jdbc.api.JdbcOption.SQL_HISTORY_ENABLED;
import static diarsid.jdbc.api.JdbcOption.SQL_HISTORY_PARAMS_REPLACE;
import static diarsid.jdbc.api.JdbcOption.TRANSACTION_GUARD_DELAY_IN_MS;
import static diarsid.jdbc.api.JdbcOption.TRANSACTION_GUARD_ENABLED;
import static diarsid.jdbc.api.JdbcOption.TRANSACTION_GUARD_SCHEDULER;
import static diarsid.support.lang.Booleans.not;

public class JdbcBuilder {

    private final SqlConnectionsSource connectionsSource;
    private final Map<JdbcOption, Object> options;

    private JdbcTransactionGuard jdbcTransactionGuard;
    private List<JdbcPreparedStatementParamSetter> additionalSetters;
    private JdbcPreparedStatementSetter setter;
    private Boolean sqlHistoryEnabled;
    private Boolean sqlHistoryParamsReplace;

    public JdbcBuilder(SqlConnectionsSource source) {
        testConnectivity(source);
        this.connectionsSource = source;
        this.options = new HashMap<>();
    }

    public JdbcBuilder(SqlConnectionsSource source, Map<JdbcOption, Object> options) {
        testConnectivity(source);
        this.connectionsSource = source;
        this.options = options;
    }

    private static void testConnectivity(SqlConnectionsSource source) {
        try (var connection = source.getConnection()) {
            LoggerFactory.getLogger(Jdbc.class).info("Connectivity OK");
        }
        catch (SQLException e) {
            throw new JdbcException("Cannot check connectivity");
        }
    }

    public Jdbc build() {
        this.configureTransactionGuard();
        this.configurePreparedStatementSetter();
        this.configureSqlLogger();
        this.configureIfSqlHistoryEnabled();
        this.configureIfReplaceParamsSqlHistoryEnabled();

        SqlTypeToJavaTypeConverter typesConverter = new SqlTypeToJavaTypeConverter(
                new SqlTimestampToSqlLocalDateTimeConversion(),
                new SqlDoubleToJavaFloatConversion());

        return new JdbcImpl(
                this.connectionsSource,
                this.jdbcTransactionGuard,
                this.setter,
                typesConverter,
                References.simplePresentOf(this.sqlHistoryEnabled),
                References.simplePresentOf(this.sqlHistoryParamsReplace));
    }

    private void configureTransactionGuard() {
        boolean enabled = this.getOptionOr(TRANSACTION_GUARD_ENABLED, Boolean.class, false);

        if ( not(enabled) ) {
            this.jdbcTransactionGuard = new JdbcTransactionGuardMock();
        }
        else {
            int msDelay = this.getOptionOr(TRANSACTION_GUARD_DELAY_IN_MS, Integer.class, 2000);
            ScheduledExecutorService scheduler = this.getOptionOr(
                    TRANSACTION_GUARD_SCHEDULER, ScheduledExecutorService.class, null);
            if ( isNull(scheduler) ) {
                scheduler = new ScheduledThreadPoolExecutor(10);
            }

            this.jdbcTransactionGuard = new JdbcTransactionGuardReal(msDelay, scheduler);
        }
    }

    private void configurePreparedStatementSetter() {
        this.additionalSetters = new ArrayList<>();

        Collection specifiedCollection = this.getOptionOr(JDBC_PREPARED_STATEMENT_SETTERS, Collection.class, null);

        if ( nonNull(specifiedCollection) ) {
            for ( Object possibleSetter : specifiedCollection ) {
                if ( possibleSetter instanceof JdbcPreparedStatementParamSetter ) {
                    this.additionalSetters.add((JdbcPreparedStatementParamSetter) possibleSetter);
                }
            }
        }

        this.setter = new JdbcPreparedStatementSetter(this.additionalSetters);
    }

    private void configureSqlLogger() {
        // TODO
    }

    private void configureIfSqlHistoryEnabled() {
        Boolean bool = this.getOptionOr(SQL_HISTORY_ENABLED, Boolean.class, true);
        this.sqlHistoryEnabled = bool;
    }

    private void configureIfReplaceParamsSqlHistoryEnabled() {
        Boolean bool = this.getOptionOr(SQL_HISTORY_PARAMS_REPLACE, Boolean.class, true);
        this.sqlHistoryParamsReplace = bool;
    }

    @SuppressWarnings("unchecked")
    private <T> T getOptionOr(JdbcOption option, Class<T> type, T defaultValue) {
        Object value = this.options.get(option);

        if ( isNull(value) ) {
            return defaultValue;
        }

        if ( type.isAssignableFrom(value.getClass()) ) {
            return (T) value;
        } else {
            return defaultValue;
        }
    }
}
