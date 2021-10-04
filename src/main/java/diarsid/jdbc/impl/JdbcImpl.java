package diarsid.jdbc.impl;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import diarsid.jdbc.api.Jdbc;
import diarsid.jdbc.api.JdbcDirectOperation;
import diarsid.jdbc.api.JdbcOption;
import diarsid.jdbc.api.JdbcTransaction;
import diarsid.jdbc.api.SqlConnectionsSource;
import diarsid.jdbc.api.ThreadBoundJdbcTransaction;
import diarsid.jdbc.api.TransactionAware;
import diarsid.jdbc.api.exceptions.ForbiddenTransactionOperation;
import diarsid.jdbc.api.exceptions.JdbcException;
import diarsid.jdbc.api.exceptions.JdbcPreparedStatementParamsException;
import diarsid.jdbc.api.sqltable.rows.Row;
import diarsid.jdbc.api.sqltable.rows.RowGetter;
import diarsid.jdbc.api.sqltable.rows.RowOperation;
import diarsid.jdbc.impl.conversion.sql2java.SqlTypeToJavaTypeConverter;
import diarsid.jdbc.impl.transaction.JdbcTransactionGuard;
import diarsid.jdbc.impl.transaction.JdbcTransactionReal;
import diarsid.support.functional.ThrowingConsumer;
import diarsid.support.functional.ThrowingFunction;
import diarsid.support.objects.references.Present;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

import static diarsid.jdbc.api.Jdbc.WhenNoTransactionThen.IF_NO_TRANSACTION_OPEN_NEW;
import static diarsid.jdbc.api.JdbcTransaction.ThenDo.CLOSE;

public class JdbcImpl implements Jdbc {

    private static final Logger logger = LoggerFactory.getLogger(Jdbc.class);

    private final SqlConnectionsSource connectionsSource;
    private final JdbcTransactionGuard transactionGuard;
    private final JdbcTransactionThreadBindingImpl threadBinding;
    private final JdbcPreparedStatementSetter paramsSetter;
    private final SqlTypeToJavaTypeConverter sqlTypeToJavaTypeConverter;
    private final Present<Boolean> sqlHistoryEnabled;
    private final Present<Boolean> replaceSqlParamsInHistory;

    public JdbcImpl(
            SqlConnectionsSource connectionsSource,
            JdbcTransactionGuard transactionGuard,
            JdbcPreparedStatementSetter paramsSetter,
            SqlTypeToJavaTypeConverter sqlTypeToJavaTypeConverter,
            Present<Boolean> sqlHistoryEnabled,
            Present<Boolean> replaceSqlParamsInHistory) {
        this.connectionsSource = connectionsSource;
        this.transactionGuard = transactionGuard;
        this.threadBinding = new JdbcTransactionThreadBindingImpl(this);
        this.paramsSetter = paramsSetter;
        this.sqlTypeToJavaTypeConverter = sqlTypeToJavaTypeConverter;
        this.sqlHistoryEnabled = sqlHistoryEnabled;
        this.replaceSqlParamsInHistory = replaceSqlParamsInHistory;
    }

    @Override
    public JdbcTransaction createTransaction() {
        if ( this.threadBinding.isBound() ) {
            throw new ForbiddenTransactionOperation(
                    "It is not allowed to nest transactions! Thread bound transaction already exists!");
        }

        try {
            JdbcTransaction tx = this.createNewTransaction();
            this.threadBinding.bindExisting(tx);
            this.setUnbindOnClose(tx);
            return tx;
        } catch (SQLException e) {
            logger.error("SQLException occured during JDBC Connection obtaining: ", e);
            throw new JdbcException(e);
        }
    }

    private void setUnbindOnClose(JdbcTransaction transaction) {
        JdbcTransactionReal real = (JdbcTransactionReal) transaction;
        real.onCloseCallback = this.threadBinding::unbind;
    }

    @Override
    public void doInTransaction(Consumer<ThreadBoundJdbcTransaction> transactionalOperation) {
        if ( this.threadBinding.isBound() ) {
            throw new ForbiddenTransactionOperation(
                    "It is not allowed to nest transactions! Thread bound transaction already exists!");
        }

        this.threadBinding.bindNew();
        JdbcTransactionReal transaction = (JdbcTransactionReal) this.threadBinding.currentTransaction();
        try {
            transactionalOperation.accept(transaction);
            transaction.commitAndClose();
        }
        catch (RuntimeException exception) {
            if ( transaction.state().isOpen() ) {
                transaction.rollbackAnd(CLOSE);
            }
            throw exception;
        }
        catch (Exception exception) {
            if ( transaction.state().isOpen() ) {
                transaction.rollbackAnd(CLOSE);
            }
            throw new JdbcException(exception);
        }
        finally {
            this.threadBinding.unbind();
        }
    }

    @Override
    public <T> T doInTransaction(Function<ThreadBoundJdbcTransaction, T> transactionalFunction) {
        if ( this.threadBinding.isBound() ) {
            throw new ForbiddenTransactionOperation(
                    "It is not allowed to nest transactions! Thread bound transaction already exists!");
        }

        this.threadBinding.bindNew();
        JdbcTransactionReal transaction = (JdbcTransactionReal) this.threadBinding.currentTransaction();
        try {
            T t = transactionalFunction.apply(transaction);
            transaction.commitAndClose();
            return t;
        }
        catch (RuntimeException exception) {
            if ( transaction.state().isOpen() ) {
                transaction.rollbackAnd(CLOSE);
            }
            throw exception;
        }
        catch (Exception exception) {
            if ( transaction.state().isOpen() ) {
                transaction.rollbackAnd(CLOSE);
            }
            throw new JdbcException(exception);
        }
        finally {
            this.threadBinding.unbind();
        }
    }

    @Override
    public void doInTransactionThrowing(ThrowingConsumer<ThreadBoundJdbcTransaction> transactionalOperation) throws Throwable {

    }

    @Override
    public <T> T doInTransactionThrowing(ThrowingFunction<ThreadBoundJdbcTransaction, T> transactionalFunction) throws Throwable {
        if ( this.threadBinding.isBound() ) {
            throw new ForbiddenTransactionOperation(
                    "It is not allowed to nest transactions! Thread bound transaction already exists!");
        }

        this.threadBinding.bindNew();
        JdbcTransactionReal transaction = (JdbcTransactionReal) this.threadBinding.currentTransaction();
        try {
            T t = transactionalFunction.applyThrowing(transaction);
            transaction.commitAndClose();
            return t;
        }
        catch (Throwable throwable) {
            if ( transaction.state().isOpen() ) {
                transaction.rollbackAnd(CLOSE);
            }
            throw throwable;
        }
        finally {
            this.threadBinding.unbind();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <P> P createTransactionalProxyFor(Class<P> type, P p, WhenNoTransactionThen then) {
        P txP = (P) Proxy.newProxyInstance(
                Jdbc.class.getClassLoader(),
                new Class[] { type },
                new TransactionalProxy(p, this, IF_NO_TRANSACTION_OPEN_NEW));

        return txP;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <P> P createTransactionalProxyFor(Class<P> type, P p, TransactionAware aware, WhenNoTransactionThen then) {
        P txP = (P) Proxy.newProxyInstance(
                Jdbc.class.getClassLoader(),
                new Class[] { type },
                new TransactionalProxy(p, aware, this, then));

        return txP;
    }

    @Override
    public JdbcTransactionThreadBindingControl threadBinding() {
        return this.threadBinding;
    }

    @Override
    public void change(JdbcOption option, Object value) {
        if ( option.isNotChangeable() ) {
            throw new JdbcException(format(
                    "%s %s is not changeable!", JdbcOption.class.getSimpleName(), option.name()));
        }


        option.mustSupportClassOf(value);

        switch ( option ) {
            case SQL_HISTORY_ENABLED: {
                boolean b = (boolean) value;
                this.sqlHistoryEnabled.resetTo(b);
                break;
            }
            case SQL_HISTORY_PARAMS_REPLACE: {
                boolean b = (boolean) value;
                this.replaceSqlParamsInHistory.resetTo(b);
                break;
            }
            default: throw option.unsupported();
        }
    }

    private JdbcTransaction createNewTransaction() throws SQLException {
        Connection connection = this.transactionConnection();

        JdbcTransactionReal transaction = new JdbcTransactionReal(
                connection,
                this.paramsSetter,
                this.sqlTypeToJavaTypeConverter,
                this.sqlHistoryEnabled.get(),
                this.replaceSqlParamsInHistory.get());

        Runnable tearDown = this.transactionGuard.accept(transaction);
        transaction.set(tearDown);

        return transaction;
    }

    private Connection autoCommittableConnection() {
        try {
            Connection connection = connectionsSource.getConnection();
            if ( ! connection.getAutoCommit() ) {
                connection.setAutoCommit(true);
            }
            return connection;
        }
        catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    private Connection transactionConnection() {
        try {
            Connection connection = connectionsSource.getConnection();
            if ( connection.getAutoCommit() ) {
                connection.setAutoCommit(false);
            }
            return connection;
        }
        catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    public void close() {
        this.transactionGuard.stop();
        this.connectionsSource.close();
        logger.info("closed.");
    }

    @Override
    public int countQueryResults(String sql) {
        try (Connection connection = this.autoCommittableConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            int resultingRowsQty = this.count(resultSet);
            resultSet.close();
            statement.close();
            return resultingRowsQty;
        }
        catch (Exception e) {
            logger.error("Exception occured during query: ");
            logger.error(sql);
            logger.error("", e);
            throw new JdbcException(e);
        }
    }

    @Override
    public int countQueryResults(String sql, Object... params) {
        return this.countQueryResultsStreamed(sql, stream(params));
    }

    @Override
    public int countQueryResults(String sql, List params) {
        return this.countQueryResultsStreamed(sql, params.stream());
    }

    private int countQueryResultsStreamed(String sql, Stream params)   {
        try (Connection connection = this.autoCommittableConnection()) {
            PreparedStatement ps = connection.prepareStatement(sql);
            this.paramsSetter.setParameters(ps, params);
            ResultSet rs = ps.executeQuery();
            int resultingRowsQty = this.count(rs);
            rs.close();
            ps.close();
            return resultingRowsQty;
        }
        catch (Exception e) {
            logger.error("Exception occured during query: ");
            logger.error(sql);
            logger.error("", e);
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
        try (Connection connection = this.autoCommittableConnection()) {
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            Row row = this.wrapResultSetIntoRow(rs);
            while ( rs.next() ) {
                operation.process(row);
            }
            ps.close();
            rs.close();
        }
        catch (Exception e) {
            logger.error("Exception occured during query: ");
            logger.error(sql);
            logger.error("", e);
            throw new JdbcException(e);
        }
    }

    @Override
    public void doQuery(RowOperation operation, String sql, List params) {
        this.doQueryStreamed(operation, sql, params.stream());
    }

    @Override
    public void doQuery(RowOperation operation, String sql, Object... params) {
        this.doQueryStreamed(operation, sql, stream(params));
    }

    private void doQueryStreamed(RowOperation operation, String sql, Stream params) {
        try (Connection connection = this.autoCommittableConnection()) {
            PreparedStatement ps = connection.prepareStatement(sql);
            this.paramsSetter.setParameters(ps, params);
            ResultSet rs = ps.executeQuery();
            Row row = this.wrapResultSetIntoRow(rs);
            while ( rs.next() ) {
                operation.process(row);
            }
            ps.close();
            rs.close();
        }
        catch (Exception e) {
            logger.error("Exception occured during query: ");
            logger.error(sql);
            logger.error("", e);
            throw new JdbcException(e);
        }
    }

    @Override
    public <T> Stream<T> doQueryAndStream(RowGetter<T> conversion, String sql) {
        try (Connection connection = this.autoCommittableConnection()) {
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery(sql);
            Row row = this.wrapResultSetIntoRow(rs);
            Stream.Builder<T> builder = Stream.builder();
            while ( rs.next() ) {
                builder.accept(conversion.getFrom(row));
            }
            st.close();
            rs.close();
            return builder.build();
        }
        catch (Exception e) {
            logger.error("Exception occured during query: ");
            logger.error(sql);
            logger.error("", e);
            throw new JdbcException(e);
        }
    }

    @Override
    public <T> Stream<T> doQueryAndStream(RowGetter<T> conversion, String sql, List params) {
        return this.doQueryAndStreamStreamed(conversion, sql, params.stream());
    }

    @Override
    public <T> Stream<T> doQueryAndStream(RowGetter<T> conversion, String sql, Object... params) {
        return this.doQueryAndStreamStreamed(conversion, sql, stream(params));
    }

    private <T> Stream<T> doQueryAndStreamStreamed(RowGetter<T> conversion, String sql, Stream params) {
        try (Connection connection = this.autoCommittableConnection()) {
            PreparedStatement ps = connection.prepareStatement(sql);
            this.paramsSetter.setParameters(ps, params);
            ResultSet rs = ps.executeQuery();
            Row row = this.wrapResultSetIntoRow(rs);
            Stream.Builder<T> builder = Stream.builder();
            while ( rs.next() ) {
                builder.accept(conversion.getFrom(row));
            }
            ps.close();
            rs.close();
            return builder.build();
        }
        catch (Exception e) {
            logger.error("Exception occured during query: ");
            logger.error(sql);
            logger.error("", e);
            throw new JdbcException(e);
        }
    }

    @Override
    public void useJdbcDirectly(JdbcDirectOperation jdbcOperation) {
        try (Connection connection = this.autoCommittableConnection()) {
            List<AutoCloseable> openedCloseables = new ArrayList<>();
            Connection proxiedConnection = SqlConnectionProxyFactory.createProxy(connection, openedCloseables);
            jdbcOperation.operateJdbcDirectly(proxiedConnection);
            for ( AutoCloseable resource : openedCloseables ) {
                resource.close();
            }
        }
        catch (Exception e) {
            logger.error(
                    "Exception occured during directly performed JDBC operation - " +
                            "exceptiond in AutoCloseable.close(): ");
            logger.error("", e);
            throw new JdbcException(e);
        }
    }

    @Override
    public void doQueryAndProcessFirstRow(RowOperation operation, String sql) {
        try (Connection connection = this.autoCommittableConnection()) {
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery(sql);
            if ( rs.first() ) {
                operation.process(this.wrapResultSetIntoRow(rs));
            }
            rs.close();
            st.close();
        }
        catch (Exception e) {
            logger.error("Exception occured during query: ");
            logger.error(sql);
            logger.error("", e);
            throw new JdbcException(e);
        }
    }

    @Override
    public void doQueryAndProcessFirstRow(RowOperation operation, String sql, List params) {
        this.doQueryAndProcessFirstRowStreamed(operation, sql, params.stream());
    }

    @Override
    public void doQueryAndProcessFirstRow(RowOperation operation, String sql, Object... params) {
        this.doQueryAndProcessFirstRowStreamed(operation, sql, stream(params));
    }

    private void doQueryAndProcessFirstRowStreamed(RowOperation operation, String sql, Stream params) {
        try (Connection connection = this.autoCommittableConnection()) {
            PreparedStatement ps = connection.prepareStatement(sql);
            this.paramsSetter.setParameters(ps, params);
            ResultSet rs = ps.executeQuery();
            if ( rs.first() ) {
                operation.process(this.wrapResultSetIntoRow(rs));
            }
            rs.close();
            ps.close();
        }
        catch (Exception e) {
            logger.error("Exception occured during query: ");
            logger.error(sql);
            logger.error("", e);
            throw new JdbcException(e);
        }
    }

    @Override
    public <T> Optional<T> doQueryAndConvertFirstRow(RowGetter<T> conversion, String sql) {
        try (Connection connection = this.autoCommittableConnection()) {
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery(sql);
            Optional<T> optional;
            if ( rs.first() ) {
                Row row = this.wrapResultSetIntoRow(rs);
                optional = Optional.ofNullable(conversion.getFrom(row));
            } else {
                optional = Optional.empty();
            }
            rs.close();
            st.close();
            return optional;
        }
        catch (Exception e) {
            logger.error("Exception occured during query: ");
            logger.error(sql);
            logger.error("", e);
            throw new JdbcException(e);
        }
    }

    @Override
    public <T> Optional<T> doQueryAndConvertFirstRow(RowGetter<T> conversion, String sql, List params) {
        return this.doQueryAndConvertFirstRowStreamed(conversion, sql, params.stream());
    }

    @Override
    public <T> Optional<T> doQueryAndConvertFirstRow(RowGetter<T> conversion, String sql, Object... params) {
        return this.doQueryAndConvertFirstRowStreamed(conversion, sql, stream(params));
    }

    private <T> Optional<T> doQueryAndConvertFirstRowStreamed(RowGetter<T> conversion, String sql, Stream params) {
        try (Connection connection = this.autoCommittableConnection()) {
            PreparedStatement ps = connection.prepareStatement(sql);
            this.paramsSetter.setParameters(ps, params);
            ResultSet rs = ps.executeQuery();
            Optional<T> optional;
            if ( rs.first() ) {
                optional = Optional.ofNullable(conversion.getFrom(this.wrapResultSetIntoRow(rs)));
            } else {
                optional = Optional.empty();
            }
            rs.close();
            ps.close();
            return optional;
        }
        catch (Exception e) {
            logger.error("Exception occured during query: ");
            logger.error(sql);
            logger.error("", e);
            throw new JdbcException(e);
        }
    }

    @Override
    public int doUpdate(String updateSql) {
        try (Connection connection = this.autoCommittableConnection()) {
            Statement ps = connection.createStatement();
            int x = ps.executeUpdate(updateSql);
            ps.close();
            return x;
        }
        catch (Exception e) {
            logger.error("Exception occured during update: ");
            logger.error(updateSql);
            logger.error("", e);
            throw new JdbcException(e);
        }
    }

    @Override
    public int doUpdate(String updateSql, List params) {
        return this.doUpdateStreamed(updateSql, params.stream());
    }

    @Override
    public int doUpdate(String updateSql, Object... params) {
        return this.doUpdateStreamed(updateSql, stream(params));
    }

    private int doUpdateStreamed(String updateSql, Stream params) {
        try (Connection connection = this.autoCommittableConnection()) {
            PreparedStatement ps = connection.prepareStatement(updateSql);
            this.paramsSetter.setParameters(ps, params);
            int x = ps.executeUpdate();
            ps.close();
            return x;
        }
        catch (Exception e) {
            logger.error("Exception occured during update: ");
            logger.error(updateSql);
            logger.error("", e);
            throw new JdbcException(e);
        }
    }

    @Override
    public int[] doBatchUpdate(String updateSql, List<List> batchParams) {
        if ( batchParams.isEmpty() ) {
            return new int[0];
        }

        this.paramsMustHaveEqualQty(batchParams, updateSql);

        try (Connection connection = this.autoCommittableConnection()) {
            PreparedStatement ps = connection.prepareStatement(updateSql);
            for (List list : batchParams) {
                this.paramsSetter.setParameters(ps, list);
                ps.addBatch();
            }
            int[] x = ps.executeBatch();
            ps.close();
            return x;
        }
        catch (Exception e) {
            logger.error("Exception occured during batch update: ");
            logger.error(updateSql);
            logger.error("...with params: ");
            for (List list : batchParams) {
                logger.error(this.concatenateParams(list));
            }
            logger.error("", e);
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

    private Row wrapResultSetIntoRow(ResultSet rs) {
        return new Row() {

            @Override
            public Object get(String columnLabel) throws JdbcException {
                try {
                    return rs.getObject(columnLabel);
                }
                catch (Exception ex) {
                    logger.error(format(
                            "Exception occured during Row processing with column: %s: ", columnLabel));
                    logger.error("", ex);
                    throw new JdbcException(ex);
                }
            }

            @SuppressWarnings("unchecked")
            @Override
            public <T> T get(String columnLabel, Class<T> type) throws JdbcException {
                try {
                    Object result = rs.getObject(columnLabel);
                    Class<?> resultType = result.getClass();
                    if ( type.equals(resultType) || type.isAssignableFrom(resultType) ) {
                        return (T) result;
                    } else {
                        return JdbcImpl
                                .this
                                .sqlTypeToJavaTypeConverter.convert(result, type);
                    }
                }
                catch (Exception ex) {
                    logger.error(format(
                            "Exception occured during Row processing with column: %s: ",
                            columnLabel));
                    logger.error("", ex);
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
                            "Exception occured during Row processing with column: %s: ", columnLabel));
                    logger.error("", ex);
                    throw new JdbcException(ex);
                }
            }
        };
    }
    
}
