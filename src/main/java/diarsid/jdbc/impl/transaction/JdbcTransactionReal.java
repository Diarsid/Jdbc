package diarsid.jdbc.impl.transaction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import diarsid.jdbc.api.JdbcDirectOperation;
import diarsid.jdbc.api.JdbcTransaction;
import diarsid.jdbc.api.SqlHistory;
import diarsid.jdbc.api.ThreadBoundJdbcTransaction;
import diarsid.jdbc.api.exceptions.ForbiddenTransactionOperation;
import diarsid.jdbc.api.exceptions.JdbcException;
import diarsid.jdbc.api.exceptions.JdbcFailureException;
import diarsid.jdbc.api.exceptions.JdbcPreparedStatementParamsException;
import diarsid.jdbc.api.exceptions.TransactionTerminationException;
import diarsid.jdbc.api.sqltable.rows.Row;
import diarsid.jdbc.api.sqltable.rows.RowGetter;
import diarsid.jdbc.api.sqltable.rows.RowOperation;
import diarsid.jdbc.impl.JdbcPreparedStatementSetter;
import diarsid.jdbc.impl.SqlConnectionProxyFactory;
import diarsid.jdbc.impl.conversion.sql2java.SqlTypeToJavaTypeConverter;
import diarsid.jdbc.impl.sqlhistory.SqlHistoryRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.time.LocalDateTime.now;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Optional.empty;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;

import static diarsid.jdbc.api.JdbcTransaction.State.CLOSED_COMMITTED;
import static diarsid.jdbc.api.JdbcTransaction.State.CLOSED_ROLLBACKED;
import static diarsid.jdbc.api.JdbcTransaction.State.FAILED;
import static diarsid.jdbc.api.JdbcTransaction.State.OPEN;
import static diarsid.jdbc.api.JdbcTransaction.ThenDo.CLOSE;
import static diarsid.jdbc.api.JdbcTransaction.ThenDo.PROCEED;
import static diarsid.support.time.TimeSupport.timeMillisAfter;


public class JdbcTransactionReal implements JdbcTransaction, ThreadBoundJdbcTransaction {
    
    private static final Logger logger = LoggerFactory.getLogger(JdbcTransactionReal.class);

    private static class RealRow implements Row {

        private final JdbcTransactionReal tx;
        private ResultSet rs;

        public RealRow(JdbcTransactionReal tx) {
            this.tx = tx;
        }

        void set(ResultSet rs) {
            this.rs = rs;
        }

        void unset() {
            this.rs = null;
        }

        @Override
        public Object get(String columnLabel) {
            try {
                return rs.getObject(columnLabel);
            }
            catch (Exception ex) {
                logger.error(format(
                        "Exception occurred during Row processing with column: %s: ", columnLabel));
                logger.error("", ex);
                this.tx.fail();
                this.tx.rollbackAnd(CLOSE);
                throw new JdbcException(ex);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T get(String columnLabel, Class<T> type) throws JdbcException {
            try {
                Object result = rs.getObject(columnLabel);

                if ( isNull(result) ) {
                    return null;
                }

                Class<?> resultType = result.getClass();
                if ( type.equals(resultType) || type.isAssignableFrom(resultType) ) {
                    return (T) result;
                } else {
                    return this.tx.sqlTypeToJavaTypeConverter.convert(result, type);
                }
            }
            catch (Exception ex) {
                logger.error(format(
                        "Exception occurred during Row processing with column: %s: ",
                        columnLabel));
                logger.error("", ex);
                this.tx.fail();
                this.tx.rollbackAnd(CLOSE);
                throw new JdbcException(ex);
            }
        }

        @Override
        public byte[] getBytes(String columnLabel) throws JdbcException {
            try {
                return rs.getBytes(columnLabel);
            }
            catch (Exception ex) {
                logger.error(format(
                        "Exception occurred during Row processing with column: %s: ", columnLabel));
                logger.error("", ex);
                this.tx.fail();
                this.tx.rollbackAnd(CLOSE);
                throw new JdbcException(ex);
            }
        }
    }
    
    private final Connection connection;
    private final UUID uuid;
    private final LocalDateTime created;
    private final JdbcPreparedStatementSetter paramsSetter;
    private final SqlTypeToJavaTypeConverter sqlTypeToJavaTypeConverter;
    private final SqlHistoryRecorder sqlHistory;
    private final boolean sqlHistoryEnabled;
    private final boolean replaceParamsInSqlHistory;
    private final RealRow row;
    private State state;
    private Runnable delayedTearDownCancel;

    public JdbcTransactionReal(
            Connection connection,
            JdbcPreparedStatementSetter argsSetter,
            SqlTypeToJavaTypeConverter sqlTypeToJavaTypeConverter,
            boolean sqlHistoryEnabled,
            boolean replaceParamsInSqlHistory) {
        this.connection = connection;
        this.uuid = randomUUID();
        this.created = now();
        this.paramsSetter = argsSetter;
        this.sqlTypeToJavaTypeConverter = sqlTypeToJavaTypeConverter;
        this.sqlHistoryEnabled = sqlHistoryEnabled;
        this.replaceParamsInSqlHistory = replaceParamsInSqlHistory;
        if ( this.sqlHistoryEnabled ) {
            this.sqlHistory = new SqlHistoryRecorder(this.uuid, replaceParamsInSqlHistory);
        }
        else {
            this.sqlHistory = null;
        }
        this.row = new RealRow(this);
        this.state = OPEN;
    }

    public void fail() {
        this.state = FAILED;
    }

    public void set(Runnable delayedTearDownCancel) {
        this.delayedTearDownCancel = delayedTearDownCancel;
    }

    private void mustBeValid() {
        if ( this.state.notEqualTo(OPEN) ) {
            throw new JdbcException("Transaction is " + this.state);
        }
    }

    @Override
    public UUID uuid() {
        return this.uuid;
    }

    @Override
    public LocalDateTime created() {
        return this.created;
    }

    /**
     * AutoCloseable interface method.
     * JdbcTransaction extends AutoCloseable in order to be legal
     * for try-with-resources usage.
     */
    @Override
    public void close() {
        try {
            if ( ! this.connection.isClosed() ) {
                if ( this.state.equalTo(OPEN) ) {
                    this.commitAndClose();
                }
                else {
                    if ( this.state.equalTo(FAILED) ) {
                        this.rollbackAnd(CLOSE);
                    }
                    else if ( this.state.equalToAny(CLOSED_COMMITTED, CLOSED_ROLLBACKED) ) {
                        // do nothing
                    }
                    else {
                        throw new UnsupportedOperationException();
                    }
                }
            }
            else {
                logger.info("closing attempt: connection has already been closed.");
            }
        }
        catch (Exception ex) {
            logger.error("exception during connection.isClosed()", ex);
            // attempt to commit anyway.
            this.commitAndClose();
        }
        finally {
            if ( this.sqlHistoryEnabled && this.sqlHistory.hasUnreported() ) {
                logger.info(this.sqlHistory.reportLast());
                this.sqlHistory.reported();
            }
        }
    }

    @Override
    public State state() {
        return this.state;
    }

    @Override
    public void doNotGuard() {
        if ( this.delayedTearDownCancel != null ) {
            this.delayedTearDownCancel.run();
        }
    }

    private void restoreAutoCommit() {
        try {
            this.connection.setAutoCommit(true);
        }
        catch (Exception e) {
            this.fail();
            logger.warn("cannot restore connection autocommit mode: ", e);
            // no actions, just proceed and try to close
            // connection.
        }
    }
    
    private void rollbackTransaction() {
        try {
            this.connection.rollback();
        }
        catch (Exception ex) {
            this.fail();
            logger.warn("cannot rollback connection: ", ex);
            // no actions, just proceed and try to close
            // connection.
        }
    }
    
    private void closeConnectionAnyway() {
        try {
            if ( ! this.connection.isClosed() ) {
                this.connection.close();
            }
            if ( nonNull(this.delayedTearDownCancel) ) {
                this.delayedTearDownCancel.run();
            }
        }
        catch (Exception e) {
            this.fail();
            logger.error("cannot close connection: ", e);
            throw new JdbcFailureException(
                    "It is impossible to close the database connection. " +
                    "Program will be closed");
        }
    }
    
    @Override
    public void rollbackAnd(ThenDo thenDo) {
        if ( isNull(thenDo) ) {
            return;
        }

        if ( this.state.equalTo(CLOSED_ROLLBACKED) ) {
            return;
        }

        if ( this.state.equalTo(CLOSED_COMMITTED) ) {
            throw new ForbiddenTransactionOperation("Transaction is already committed!");
        }

        long start = currentTimeMillis();
        this.rollbackTransaction();
        long duration = timeMillisAfter(start);

        if ( this.sqlHistoryEnabled && this.sqlHistory.hasUnreported() ) {
            this.sqlHistory.addRollback(duration);
            logger.info(this.sqlHistory.reportLast());
            this.sqlHistory.reported();
        }

        switch ( thenDo ) {
            case PROCEED:
                break;
            case CLOSE:
                this.state = CLOSED_ROLLBACKED;
                this.restoreAutoCommit();
                this.closeConnectionAnyway();
                break;
            case THROW:
                this.state = CLOSED_ROLLBACKED;
                this.restoreAutoCommit();
                this.closeConnectionAnyway();
                throw new TransactionTerminationException("transaction has been terminated normally.");
            default:
                throw new UnsupportedOperationException(thenDo.name() +" is not supported!");
        }
    }

    @Override
    public void rollbackAndProceed() {
        this.rollbackAnd(PROCEED);
    }
    
    @Override
    public int countQueryResults(String sql) {
        this.mustBeValid();
        long start = currentTimeMillis();

        try {
            Statement statement = this.connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            int resultingRowsQty = this.count(resultSet);
            resultSet.close();
            statement.close();

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, millis);
            }

            return resultingRowsQty;
        }
        catch (Exception e) {
            logger.error("Exception occurred during query: ");
            logger.error(sql);
            logger.error("", e);

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, millis);
                this.sqlHistory.add(e);
            }

            this.fail();
            this.rollbackAnd(CLOSE);
            throw new JdbcException(e);
        }         
    }
    
    @Override
    public int countQueryResults(String sql, Object... params) {
        this.mustBeValid();
        long start = currentTimeMillis();

        try {
            PreparedStatement ps = this.connection.prepareStatement(sql);
            this.paramsSetter.setParameters(ps, params);
            ResultSet rs = ps.executeQuery();
            int resultingRowsQty = this.count(rs);
            rs.close();
            ps.close();

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, params, millis);
            }

            return resultingRowsQty;
        }
        catch (Exception e) {
            logger.error("Exception occurred during query: ");
            logger.error(sql);
            logger.error("", e);

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, params, millis);
                this.sqlHistory.add(e);
            }

            this.fail();
            this.rollbackAnd(CLOSE);
            throw new JdbcException(e);
        }
    }
    
    @Override
    public int countQueryResults(String sql, List params) {
        this.mustBeValid();
        long start = currentTimeMillis();

        try {
            PreparedStatement ps = this.connection.prepareStatement(sql);
            this.paramsSetter.setParameters(ps, params);
            ResultSet rs = ps.executeQuery();
            int resultingRowsQty = this.count(rs);
            rs.close();
            ps.close();

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, params, millis);
            }

            return resultingRowsQty;
        }
        catch (Exception e) {
            logger.error("Exception occurred during query: ");
            logger.error(sql);
            logger.error("", e);

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, params, millis);
                this.sqlHistory.add(e);
            }

            this.fail();
            this.rollbackAnd(CLOSE);
            throw new JdbcException(e);
        }
    }
    
    private int count(ResultSet rs) throws SQLException {
        int count = 0;
        while ( rs.next() ) {            
            count++;
        }
        return count;
    }
    
    private String concatenateParams(List<Object> params) {
        return params.stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));
    }
    
    @Override
    public void doQuery(RowOperation operation, String sql) {
        this.mustBeValid();
        long start = currentTimeMillis();

        try {
            PreparedStatement ps = this.connection.prepareStatement(sql);
            this.paramsSetter.setParameters(ps);
            ResultSet rs = ps.executeQuery();
            this.row.set(rs);
            while ( rs.next() ) {
                operation.process(this.row);
            }
            this.row.unset();
            ps.close();
            rs.close();

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, millis);
            }
        }
        catch (Exception e) {
            logger.error("Exception occurred during query: ");
            logger.error(sql);
            logger.error("", e);

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, millis);
                this.sqlHistory.add(e);
            }

            this.fail();
            this.rollbackAnd(CLOSE);
            throw new JdbcException(e);
        }
    }
    
    @Override
    public void doQuery(RowOperation operation, String sql, List params) {
        this.mustBeValid();
        long start = currentTimeMillis();

        try {
            PreparedStatement ps = this.connection.prepareStatement(sql);
            this.paramsSetter.setParameters(ps, params);
            ResultSet rs = ps.executeQuery();
            this.row.set(rs);
            while ( rs.next() ) {
                operation.process(this.row);
            }
            this.row.unset();
            ps.close();
            rs.close();

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, params, millis);
            }
        }
        catch (Exception e) {
            logger.error("Exception occurred during query: ");
            logger.error(sql);
            logger.error("", e);

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, params, millis);
                this.sqlHistory.add(e);
            }

            this.fail();
            this.rollbackAnd(CLOSE);
            throw new JdbcException(e);
        }
    }
    
    @Override
    public void doQuery(RowOperation operation, String sql, Object... params) {
        this.mustBeValid();
        long start = currentTimeMillis();

        try {
            PreparedStatement ps = this.connection.prepareStatement(sql);
            this.paramsSetter.setParameters(ps, params);
            ResultSet rs = ps.executeQuery();
            this.row.set(rs);
            while ( rs.next() ) {
                operation.process(this.row);
            }
            this.row.unset();
            ps.close();
            rs.close();

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, params, millis);
            }
        }
        catch (Exception e) {
            logger.error("Exception occurred during query: ");
            logger.error(sql);
            logger.error("", e);

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, params, millis);
                this.sqlHistory.add(e);
            }

            this.fail();
            this.rollbackAnd(CLOSE);
            throw new JdbcException(e);
        }
    }
    
    @Override
    public <T> Stream<T> doQueryAndStream(RowGetter<T> conversion, String sql) {
        this.mustBeValid();
        long start = currentTimeMillis();

        try {
            Statement st = this.connection.createStatement();
            ResultSet rs = st.executeQuery(sql);
            this.row.set(rs);
            Stream.Builder<T> builder = Stream.builder();
            while ( rs.next() ) {
                builder.accept(conversion.getFrom(this.row));
            }
            this.row.unset();
            st.close();
            rs.close();

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, millis);
            }

            return builder.build();
        }
        catch (Exception e) {
            logger.error("Exception occurred during query: ");
            logger.error(sql);
            logger.error("", e);

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, millis);
                this.sqlHistory.add(e);
            }

            this.fail();
            this.rollbackAnd(CLOSE);
            throw new JdbcException(e);
        }  
    }
    
    @Override
    public <T> Stream<T> doQueryAndStream(RowGetter<T> conversion, String sql, List params) {
        this.mustBeValid();
        long start = currentTimeMillis();

        try {
            PreparedStatement ps = this.connection.prepareStatement(sql);
            this.paramsSetter.setParameters(ps, params);
            ResultSet rs = ps.executeQuery();
            this.row.set(rs);
            Stream.Builder<T> builder = Stream.builder();
            while ( rs.next() ) {
                builder.accept(conversion.getFrom(this.row));
            }
            this.row.unset();
            ps.close();
            rs.close();

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, params, millis);
            }

            return builder.build();
        }
        catch (Exception e) {
            logger.error("Exception occurred during query: ");
            logger.error(sql);
            logger.error("", e);

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, params, millis);
                this.sqlHistory.add(e);
            }

            this.fail();
            this.rollbackAnd(CLOSE);
            throw new JdbcException(e);
        }
    }
    
    @Override
    public <T> Stream<T> doQueryAndStream(RowGetter<T> conversion, String sql, Object... params) {
        this.mustBeValid();
        long start = currentTimeMillis();

        try {
            PreparedStatement ps = this.connection.prepareStatement(sql);
            this.paramsSetter.setParameters(ps, params);
            ResultSet rs = ps.executeQuery();
            this.row.set(rs);
            Stream.Builder<T> builder = Stream.builder();
            while ( rs.next() ) {
                builder.accept(conversion.getFrom(this.row));
            }
            this.row.unset();
            ps.close();
            rs.close();

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, params, millis);
            }

            return builder.build();
        }
        catch (Exception e) {
            logger.error("Exception occurred during query: ");
            logger.error(sql);
            logger.error("", e);

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, params, millis);
                this.sqlHistory.add(e);
            }

            this.fail();
            this.rollbackAnd(CLOSE);
            throw new JdbcException(e);
        }
    }
    
    @Override
    public void useJdbcDirectly(JdbcDirectOperation jdbcOperation) {
        this.mustBeValid();
        long start = currentTimeMillis();

        this.sqlHistory.add(
                "[DIRECT JDBC OPERATION] " +
                "sql history is unreacheable for this operation.");
        try {
            List<AutoCloseable> openedCloseables = new ArrayList<>();
            Connection proxiedConnection = SqlConnectionProxyFactory.createProxy(this.connection, openedCloseables);
            jdbcOperation.operateJdbcDirectly(proxiedConnection);
            for ( AutoCloseable resource : openedCloseables ) {
                resource.close();
            }
        }
        catch (Exception e) {
            logger.error(
                    "Exception occurred during directly performed JDBC operation - " +
                    "exceptiond in AutoCloseable.close(): ");
            logger.error("", e);

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(e);
            }

            this.fail();
            this.rollbackAnd(CLOSE);
            throw new JdbcException(e);
        } 
    }
    
    @Override
    public void doQueryAndProcessFirstRow(RowOperation operation, String sql) {
        this.mustBeValid();
        long start = currentTimeMillis();

        try {
            PreparedStatement ps = this.connection.prepareStatement(sql);
            this.paramsSetter.setParameters(ps);
            ResultSet rs = ps.executeQuery();
            this.row.set(rs);
            if ( rs.first() ) {
                operation.process(this.row);
            }
            this.row.unset();
            rs.close();
            ps.close();

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, millis);
            }
        }
        catch (Exception e) {
            logger.error("Exception occurred during query: ");
            logger.error(sql);
            logger.error("", e);

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, millis);
                this.sqlHistory.add(e);
            }

            this.fail();
            this.rollbackAnd(CLOSE);
            throw new JdbcException(e);
        }
    }
    
    @Override
    public void doQueryAndProcessFirstRow(RowOperation operation, String sql, List params) {
        this.mustBeValid();
        long start = currentTimeMillis();

        try {
            PreparedStatement ps = this.connection.prepareStatement(sql);
            this.paramsSetter.setParameters(ps, params);
            ResultSet rs = ps.executeQuery();
            this.row.set(rs);
            if ( rs.first() ) {
                operation.process(this.row);
            }
            this.row.unset();
            rs.close();
            ps.close();

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, params, millis);
            }
        }
        catch (Exception e) {
            logger.error("Exception occurred during query: ");
            logger.error(sql);
            logger.error("", e);

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, params, millis);
                this.sqlHistory.add(e);
            }

            this.fail();
            this.rollbackAnd(CLOSE);
            throw new JdbcException(e);
        }
    }
    
    @Override
    public void doQueryAndProcessFirstRow(RowOperation operation, String sql, Object... params) {
        this.mustBeValid();
        long start = currentTimeMillis();

        try {
            PreparedStatement ps = this.connection.prepareStatement(sql);
            this.paramsSetter.setParameters(ps, params);
            ResultSet rs = ps.executeQuery();
            this.row.set(rs);
            if ( rs.first() ) {
                operation.process(this.row);
            }
            this.row.unset();
            rs.close();
            ps.close();

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, params, millis);
            }
        }
        catch (Exception e) {
            logger.error("Exception occurred during query: ");
            logger.error(sql);
            logger.error("", e);

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, params, millis);
                this.sqlHistory.add(e);
            }

            this.fail();
            this.rollbackAnd(CLOSE);
            throw new JdbcException(e);
        }
    }
    
    @Override
    public <T> Optional<T> doQueryAndConvertFirstRow(RowGetter<T> conversion, String sql) {
        this.mustBeValid();
        long start = currentTimeMillis();

        try {
            PreparedStatement ps = this.connection.prepareStatement(sql);
            this.paramsSetter.setParameters(ps);
            ResultSet rs = ps.executeQuery();
            Optional<T> optional;
            this.row.set(rs);
            if ( rs.first() ) {
                optional = Optional.ofNullable(conversion.getFrom(this.row));
            } else {
                optional = empty();
            }
            this.row.unset();
            rs.close();
            ps.close();

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, millis);
            }

            return optional;
        }
        catch (Exception e) {
            logger.error("Exception occurred during query: ");
            logger.error(sql);
            logger.error("", e);

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, millis);
                this.sqlHistory.add(e);
            }

            this.fail();
            this.rollbackAnd(CLOSE);
            throw new JdbcException(e);
        }
    }
    
    @Override
    public <T> Optional<T> doQueryAndConvertFirstRow(RowGetter<T> conversion, String sql, List params) {
        this.mustBeValid();
        long start = currentTimeMillis();

        try {
            PreparedStatement ps = this.connection.prepareStatement(sql);
            this.paramsSetter.setParameters(ps, params);
            ResultSet rs = ps.executeQuery();
            Optional<T> optional;
            this.row.set(rs);
            if ( rs.first() ) {
                optional = Optional.ofNullable(conversion.getFrom(this.row));
            } else {
                optional = empty();
            }
            this.row.unset();
            rs.close();
            ps.close();

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, params, millis);
            }

            return optional;
        }
        catch (Exception e) {
            logger.error("Exception occurred during query: ");
            logger.error(sql);
            logger.error("", e);

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, params, millis);
                this.sqlHistory.add(e);
            }

            this.fail();
            this.rollbackAnd(CLOSE);
            throw new JdbcException(e);
        }
    }
    
    @Override
    public <T> Optional<T> doQueryAndConvertFirstRow(RowGetter<T> conversion, String sql, Object... params) {
        this.mustBeValid();
        long start = currentTimeMillis();

        try {
            PreparedStatement ps = this.connection.prepareStatement(sql);
            this.paramsSetter.setParameters(ps, params);
            ResultSet rs = ps.executeQuery();
            Optional<T> optional;
            this.row.set(rs);
            if ( rs.first() ) {
                optional = Optional.ofNullable(conversion.getFrom(this.row));
            } else {
                optional = empty();
            }
            this.row.unset();
            rs.close();
            ps.close();

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, params, millis);
            }

            return optional;
        }
        catch (Exception e) {
            logger.error("Exception occurred during query: ");
            logger.error(sql);
            logger.error("", e);

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(sql, params, millis);
                this.sqlHistory.add(e);
            }

            this.fail();
            this.rollbackAnd(CLOSE);
            throw new JdbcException(e);
        }
    }
    
    @Override
    public int doUpdate(String updateSql) {
        this.mustBeValid();
        long start = currentTimeMillis();

        try {
            Statement ps = this.connection.createStatement();
            int x = ps.executeUpdate(updateSql);
            ps.close();

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(updateSql, millis);
            }

            return x;
        }
        catch (Exception e) {
            logger.error("Exception occurred during update: ");
            logger.error(updateSql);
            logger.error("", e);

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(updateSql, millis);
                this.sqlHistory.add(e);
            }

            this.fail();
            this.rollbackAnd(CLOSE);
            throw new JdbcException(e);
        }  
    }
    
    @Override
    public int doUpdate(String updateSql, List params) {
        this.mustBeValid();
        long start = currentTimeMillis();

        try {
            PreparedStatement ps = this.connection.prepareStatement(updateSql);
            this.paramsSetter.setParameters(ps, params);
            int x = ps.executeUpdate();
            ps.close();

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(updateSql, params, millis);
            }

            return x;
        }
        catch (Exception e) {
            logger.error("Exception occurred during update: ");
            logger.error(updateSql);
            logger.error("", e);

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(updateSql, params, millis);
                this.sqlHistory.add(e);
            }

            this.fail();
            this.rollbackAnd(CLOSE);
            throw new JdbcException(e);
        }
    }
    
    @Override
    public int doUpdate(String updateSql, Object... params) {
        this.mustBeValid();
        long start = currentTimeMillis();

        try {
            PreparedStatement ps = this.connection.prepareStatement(updateSql);
            this.paramsSetter.setParameters(ps, params);
            int x = ps.executeUpdate();
            ps.close();

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(updateSql, params, millis);
            }

            return x;
        }
        catch (Exception e) {
            logger.error("Exception occurred during update: ");
            logger.error(updateSql);
            logger.error("", e);

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.add(updateSql, params, millis);
                this.sqlHistory.add(e);
            }

            this.fail();
            this.rollbackAnd(CLOSE);
            throw new JdbcException(e);
        }
    }
    
    @Override
    public int[] doBatchUpdate(String updateSql, List<List> batchParams) {
        this.mustBeValid();
        if ( batchParams.isEmpty() ) {
            return new int[0];
        }
        long start = currentTimeMillis();

        this.paramsMustHaveEqualQty(batchParams, updateSql);

        try {
            PreparedStatement ps = this.connection.prepareStatement(updateSql);
            for ( List params : batchParams ) {
                this.paramsSetter.setParameters(ps, params);
                ps.addBatch();
            }           
            int[] x = ps.executeBatch();
            ps.close();

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.addBatch(updateSql, batchParams, millis);
            }

            return x;
        }
        catch (Exception e) {
            logger.error("Exception occurred during batch update: ");
            logger.error(updateSql);
            logger.error("...with params: ");
            for ( List params : batchParams ) {
                logger.error(this.concatenateParams(params));
            }
            logger.error("", e);

            if ( this.sqlHistoryEnabled ) {
                long millis = timeMillisAfter(start);
                this.sqlHistory.addBatch(updateSql, batchParams, millis);
                this.sqlHistory.add(e);
            }

            this.fail();
            this.rollbackAnd(CLOSE);
            throw new JdbcException(e);
        }  
    }

    private void paramsMustHaveEqualQty(List<List> batchParams, String updateSql) {
        int paramsQty = batchParams.iterator().next().size();
        batchParams
                .stream()
                .filter(params -> params.size() != paramsQty)
                .findFirst()
                .ifPresent(params -> this.paramsQtyAreDifferent(updateSql));
    }

    private int paramsMustHaveEvenQty(List batchParams, int batchesQty, String updateSql) {
        int paramsQtyInBatch = batchParams.size() % batchesQty;

        if ( paramsQtyInBatch != 0 ) {
            throw new JdbcPreparedStatementParamsException(
                    format("PreparedStatement parameters qty differs for SQL: %s", updateSql));
        }

        return paramsQtyInBatch;
    }
    
    private void paramsQtyAreDifferent(String sql) {
        throw new JdbcPreparedStatementParamsException(
                format("PreparedStatement parameters qty differs for SQL: %s", sql));
    }
    
    @Override
    public int[] doBatchUpdate(String updateSql, List... batchParams) {
        return this.doBatchUpdate(updateSql, asList(batchParams));
    }

    @Override
    public <T> int[] doBatchUpdate(String updateSql, ArgsFrom<T> argsFromT, List<T> tObjects) {
        List<List> args = tObjects.stream().map(argsFromT).collect(toList());
        return this.doBatchUpdate(updateSql, args);
    }

    @Override
    public <T> int[] doBatchUpdate(String updateSql, ArgsFrom<T> argsFromT, T... tObjects) {
        List<List> args = stream(tObjects).map(argsFromT).collect(toList());
        return this.doBatchUpdate(updateSql, args);
    }

    @Override
    public void commitAndClose() {
        if ( this.state.equalTo(CLOSED_COMMITTED) || this.state.equalTo(CLOSED_ROLLBACKED) ) {
            throw new JdbcException("Transaction is already closed!");
        }

        boolean rollbackInsteadOfCommit = false;

        try {
            if ( this.state.equalTo(OPEN) ) {
                this.connection.commit();
                this.state = CLOSED_COMMITTED;
            }
            else if ( this.state.equalTo(FAILED) ) {
                this.connection.rollback();
                this.state = CLOSED_ROLLBACKED;
                rollbackInsteadOfCommit = true;
            }
        }
        catch (Exception commitException) {
            this.fail();
            logger.error("Exception occurred during commiting: ");
            logger.error("", commitException);
            try {
                this.connection.rollback();
                logger.error("transaction has been rolled back.");
            }
            catch (Exception rollbackException) {
                logger.error("Exception occurred during rollback of connection failed to commit: ");
                logger.error("", rollbackException);
                // No actions after rollbackAndTerminate has failed.
                // Go to finally block and finish transaction.
            }
        }
        finally {
            this.restoreAutoCommit();
            this.closeConnectionAnyway();
            if ( this.sqlHistoryEnabled && this.sqlHistory.hasUnreported() ) {
                logger.info(this.sqlHistory.reportLast());
                this.sqlHistory.reported();
            }
        }

        if ( rollbackInsteadOfCommit ) {
            throw new JdbcException("Transaction is failed and was rolled back instead of commit!");
        }
    }
    
    @Override
    public SqlHistory sqlHistory() {
        return this.sqlHistory;
    }
}
