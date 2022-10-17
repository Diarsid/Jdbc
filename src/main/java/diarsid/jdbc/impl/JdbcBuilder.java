package diarsid.jdbc.impl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.LoggerFactory;

import diarsid.jdbc.api.Jdbc;
import diarsid.jdbc.api.JdbcOption;
import diarsid.jdbc.api.SqlConnectionsSource;
import diarsid.jdbc.api.exceptions.JdbcException;
import diarsid.jdbc.impl.conversion.sql2java.SqlDoubleToJavaFloatConversion;
import diarsid.jdbc.impl.conversion.sql2java.SqlTimestampToSqlLocalDateTimeConversion;
import diarsid.jdbc.impl.conversion.sql2java.SqlTypeToJavaTypeConverter;
import diarsid.support.objects.references.References;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import static diarsid.jdbc.api.JdbcOption.JDBC_PREPARED_STATEMENT_SETTERS;
import static diarsid.jdbc.api.JdbcOption.SQL_HISTORY_ENABLED;
import static diarsid.jdbc.api.JdbcOption.SQL_HISTORY_PARAMS_REPLACE;

public class JdbcBuilder {

    private final SqlConnectionsSource connectionsSource;
    private final Map<JdbcOption, Object> options;

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
        this.configurePreparedStatementSetter();
        this.configureSqlLogger();
        this.configureIfSqlHistoryEnabled();
        this.configureIfReplaceParamsSqlHistoryEnabled();

        SqlTypeToJavaTypeConverter typesConverter = new SqlTypeToJavaTypeConverter(
                new SqlTimestampToSqlLocalDateTimeConversion(),
                new SqlDoubleToJavaFloatConversion());

        return new JdbcImpl(
                this.connectionsSource,
                this.setter,
                typesConverter,
                References.simplePresentOf(this.sqlHistoryEnabled),
                References.simplePresentOf(this.sqlHistoryParamsReplace));
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
