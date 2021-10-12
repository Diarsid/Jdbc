package diarsid.jdbc.impl;

import java.io.Closeable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import diarsid.jdbc.api.JdbcOperations;
import diarsid.jdbc.api.exceptions.ForbiddenTransactionOperation;
import diarsid.jdbc.api.exceptions.JdbcPreparedStatementParamsException;
import diarsid.jdbc.impl.params.ParamSetterBinaryStream;
import diarsid.jdbc.impl.params.ParamSetterBool;
import diarsid.jdbc.impl.params.ParamSetterByteArray;
import diarsid.jdbc.impl.params.ParamSetterCharacter;
import diarsid.jdbc.impl.params.ParamSetterDouble;
import diarsid.jdbc.impl.params.ParamSetterEnum;
import diarsid.jdbc.impl.params.ParamSetterFloat;
import diarsid.jdbc.impl.params.ParamSetterInt;
import diarsid.jdbc.impl.params.ParamSetterLocalDateTime;
import diarsid.jdbc.impl.params.ParamSetterLong;
import diarsid.jdbc.impl.params.ParamSetterNull;
import diarsid.jdbc.impl.params.ParamSetterString;
import diarsid.jdbc.impl.params.ParamSetterUUID;
import diarsid.support.objects.collections.StreamsSupport;

import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toList;

public class JdbcPreparedStatementSetter implements JdbcOperations.Params {

    private final List<JdbcPreparedStatementParamSetter> setters;
    private final ThreadLocal<PreparedStatement> localStatement;
    private final ThreadLocal<AtomicInteger> localIndex;
    private final int settersQty;

    public JdbcPreparedStatementSetter(
            List<JdbcPreparedStatementParamSetter> additionalSetters) {
        Set<JdbcPreparedStatementParamSetter> defaultSetters = new HashSet<>();
        defaultSetters.add(new ParamSetterNull());
        defaultSetters.add(new ParamSetterString());
        defaultSetters.add(new ParamSetterBool());
        defaultSetters.add(new ParamSetterInt());
        defaultSetters.add(new ParamSetterLong());
        defaultSetters.add(new ParamSetterByteArray());
        defaultSetters.add(new ParamSetterBinaryStream());
        defaultSetters.add(new ParamSetterLocalDateTime());
        defaultSetters.add(new ParamSetterEnum());
        defaultSetters.add(new ParamSetterUUID());
        defaultSetters.add(new ParamSetterFloat());
        defaultSetters.add(new ParamSetterDouble());
        defaultSetters.add(new ParamSetterCharacter());

        if ( nonNull(additionalSetters) && additionalSetters.size() > 0 ) {
            defaultSetters.addAll(additionalSetters);
        }

        this.setters = new ArrayList<>(defaultSetters);
        this.settersQty = this.setters.size();

        this.localStatement = new ThreadLocal<>();
        this.localIndex = new ThreadLocal<>();
    }
    
    public Closeable setParameters(PreparedStatement statement, Stream<Object> params) throws SQLException {
        int paramIndex = 1;
        for ( Object param : StreamsSupport.unwrap(params).collect(toList()) ) {
            this.findAppropriateSetterFor(param).setParameterInto(statement, paramIndex, param);
            paramIndex++;
        }

        return CloseableStub.INSTANCE;
    }

    public <T> Closeable apply(PreparedStatement statement, T t, JdbcOperations.ParamsApplier<T> paramsApplier) throws SQLException {
        this.localStatement.set(statement);
        this.localIndex.set(new AtomicInteger(1));
        try {
            paramsApplier.apply(t, this);
        }
        finally {
            this.localStatement.remove();
        }

        return CloseableStub.INSTANCE;
    }

    @Override
    public void add(Object param) {
        PreparedStatement ps = this.localStatement.get();
        int index = this.localIndex.get().getAndIncrement();

        if ( isNull(ps) ) {
            throw new JdbcPreparedStatementParamsException(
                    "PreparedStatement is not set for thread  " + currentThread().getName());
        }

        try {
            this.findAppropriateSetterFor(param).setParameterInto(ps, index, param);
        }
        catch (SQLException e) {
            throw new JdbcPreparedStatementParamsException(
                    format("Cannot set %s:%s param into PreparedStatement: %s", index, param, e.getMessage()));
        }
    }

    public Closeable setParameters(PreparedStatement statement, Object param1) throws SQLException {
        this.findAppropriateSetterFor(param1).setParameterInto(statement, 1, param1);

        return CloseableStub.INSTANCE;
    }

    public Closeable setParameters(PreparedStatement statement, Object param1, Object param2) throws SQLException {
        this.findAppropriateSetterFor(param1).setParameterInto(statement, 1, param1);
        this.findAppropriateSetterFor(param2).setParameterInto(statement, 2, param2);

        return CloseableStub.INSTANCE;
    }

    public Closeable setParameters(PreparedStatement statement, Object param1, Object param2, Object param3) throws SQLException {
        this.findAppropriateSetterFor(param1).setParameterInto(statement, 1, param1);
        this.findAppropriateSetterFor(param2).setParameterInto(statement, 2, param2);
        this.findAppropriateSetterFor(param3).setParameterInto(statement, 3, param3);

        return CloseableStub.INSTANCE;
    }

    public Closeable setParameters(PreparedStatement statement, Object param1, Object param2, Object param3, Object param4) throws SQLException {
        this.findAppropriateSetterFor(param1).setParameterInto(statement, 1, param1);
        this.findAppropriateSetterFor(param2).setParameterInto(statement, 2, param2);
        this.findAppropriateSetterFor(param3).setParameterInto(statement, 3, param3);
        this.findAppropriateSetterFor(param4).setParameterInto(statement, 4, param4);

        return CloseableStub.INSTANCE;
    }

    public Closeable setParameters(PreparedStatement statement, Object param1, Object param2, Object param3, Object param4, Object param5) throws SQLException {
        this.findAppropriateSetterFor(param1).setParameterInto(statement, 1, param1);
        this.findAppropriateSetterFor(param2).setParameterInto(statement, 2, param2);
        this.findAppropriateSetterFor(param3).setParameterInto(statement, 3, param3);
        this.findAppropriateSetterFor(param4).setParameterInto(statement, 4, param4);
        this.findAppropriateSetterFor(param5).setParameterInto(statement, 5, param5);

        return CloseableStub.INSTANCE;
    }

    public Closeable setParameters(PreparedStatement statement, Object param1, Object param2, Object param3, Object param4, Object param5, Object param6) throws SQLException {
        this.findAppropriateSetterFor(param1).setParameterInto(statement, 1, param1);
        this.findAppropriateSetterFor(param2).setParameterInto(statement, 2, param2);
        this.findAppropriateSetterFor(param3).setParameterInto(statement, 3, param3);
        this.findAppropriateSetterFor(param4).setParameterInto(statement, 4, param4);
        this.findAppropriateSetterFor(param5).setParameterInto(statement, 5, param5);
        this.findAppropriateSetterFor(param6).setParameterInto(statement, 6, param6);

        return CloseableStub.INSTANCE;
    }

    public Closeable setParameters(PreparedStatement statement, Object param1, Object param2, Object param3, Object param4, Object param5, Object param6, Object param7) throws SQLException {
        this.findAppropriateSetterFor(param1).setParameterInto(statement, 1, param1);
        this.findAppropriateSetterFor(param2).setParameterInto(statement, 2, param2);
        this.findAppropriateSetterFor(param3).setParameterInto(statement, 3, param3);
        this.findAppropriateSetterFor(param4).setParameterInto(statement, 4, param4);
        this.findAppropriateSetterFor(param5).setParameterInto(statement, 5, param5);
        this.findAppropriateSetterFor(param6).setParameterInto(statement, 6, param6);
        this.findAppropriateSetterFor(param7).setParameterInto(statement, 7, param7);

        return CloseableStub.INSTANCE;
    }

    public Closeable setParameters(PreparedStatement statement, Object param1, Object param2, Object param3, Object param4, Object param5, Object param6, Object param7, Object param8) throws SQLException {
        this.findAppropriateSetterFor(param1).setParameterInto(statement, 1, param1);
        this.findAppropriateSetterFor(param2).setParameterInto(statement, 2, param2);
        this.findAppropriateSetterFor(param3).setParameterInto(statement, 3, param3);
        this.findAppropriateSetterFor(param4).setParameterInto(statement, 4, param4);
        this.findAppropriateSetterFor(param5).setParameterInto(statement, 5, param5);
        this.findAppropriateSetterFor(param6).setParameterInto(statement, 6, param6);
        this.findAppropriateSetterFor(param7).setParameterInto(statement, 7, param7);
        this.findAppropriateSetterFor(param8).setParameterInto(statement, 8, param8);

        return CloseableStub.INSTANCE;
    }

    public Closeable setParameters(PreparedStatement statement, Object param1, Object param2, Object param3, Object param4, Object param5, Object param6, Object param7, Object param8, Object param9) throws SQLException {
        this.findAppropriateSetterFor(param1).setParameterInto(statement, 1, param1);
        this.findAppropriateSetterFor(param2).setParameterInto(statement, 2, param2);
        this.findAppropriateSetterFor(param3).setParameterInto(statement, 3, param3);
        this.findAppropriateSetterFor(param4).setParameterInto(statement, 4, param4);
        this.findAppropriateSetterFor(param5).setParameterInto(statement, 5, param5);
        this.findAppropriateSetterFor(param6).setParameterInto(statement, 6, param6);
        this.findAppropriateSetterFor(param7).setParameterInto(statement, 7, param7);
        this.findAppropriateSetterFor(param9).setParameterInto(statement, 9, param9);

        return CloseableStub.INSTANCE;
    }

    public Closeable setParameters(PreparedStatement statement, Object param1, Object param2, Object param3, Object param4, Object param5, Object param6, Object param7, Object param8, Object param9, Object param10) throws SQLException {
        this.findAppropriateSetterFor(param1).setParameterInto(statement, 1, param1);
        this.findAppropriateSetterFor(param2).setParameterInto(statement, 2, param2);
        this.findAppropriateSetterFor(param3).setParameterInto(statement, 3, param3);
        this.findAppropriateSetterFor(param4).setParameterInto(statement, 4, param4);
        this.findAppropriateSetterFor(param5).setParameterInto(statement, 5, param5);
        this.findAppropriateSetterFor(param6).setParameterInto(statement, 6, param6);
        this.findAppropriateSetterFor(param7).setParameterInto(statement, 7, param7);
        this.findAppropriateSetterFor(param9).setParameterInto(statement, 9, param9);
        this.findAppropriateSetterFor(param10).setParameterInto(statement, 10, param10);

        return CloseableStub.INSTANCE;
    }

    public Closeable setParameters(PreparedStatement statement, Object... params) throws SQLException {
        int paramIndex = 1;
        for ( Object param : params ) {
            if ( param instanceof Collection ) {
                Collection nestedParams = (Collection) param;
                for ( Object nestedParam : nestedParams ) {
                    if ( nestedParam instanceof Collection || nestedParam instanceof Object[] ) {
                        throw new ForbiddenTransactionOperation("Params nested more than one level are not supported");
                    }
                    this.findAppropriateSetterFor(nestedParam).setParameterInto(statement, paramIndex, nestedParam);
                    paramIndex++;
                }
            }
            else if ( param instanceof Object[] ) {
                Object[] nestedParams = (Object[]) param;
                for ( Object nestedParam : nestedParams ) {
                    if ( nestedParam instanceof Collection || nestedParam instanceof Object[] ) {
                        throw new ForbiddenTransactionOperation("Params nested more than one level are not supported");
                    }
                    this.findAppropriateSetterFor(nestedParam).setParameterInto(statement, paramIndex, nestedParam);
                    paramIndex++;
                }
            }
            else {
                this.findAppropriateSetterFor(param).setParameterInto(statement, paramIndex, param);
                paramIndex++;
            }
        }

        return CloseableStub.INSTANCE;
    }

    public Closeable setParameters(PreparedStatement statement, List params) throws SQLException {
        int paramIndex = 1;
        for ( Object param : params ) {
            if ( param instanceof Collection ) {
                Collection nestedParams = (Collection) param;
                for ( Object nestedParam : nestedParams ) {
                    if ( nestedParam instanceof Collection || nestedParam instanceof Object[] ) {
                        throw new ForbiddenTransactionOperation("Params nested more than one level are not supported");
                    }
                    this.findAppropriateSetterFor(param).setParameterInto(statement, paramIndex, nestedParam);
                    paramIndex++;
                }
            }
            else if ( param instanceof Object[] ) {
                Object[] nestedParams = (Object[]) param;
                for ( Object nestedParam : nestedParams ) {
                    if ( nestedParam instanceof Collection || nestedParam instanceof Object[] ) {
                        throw new ForbiddenTransactionOperation("Params nested more than one level are not supported");
                    }
                    this.findAppropriateSetterFor(param).setParameterInto(statement, paramIndex, nestedParam);
                    paramIndex++;
                }
            }
            else {
                this.findAppropriateSetterFor(param).setParameterInto(statement, paramIndex, param);
                paramIndex++;
            }
        }

        return CloseableStub.INSTANCE;
    }
    
    private JdbcPreparedStatementParamSetter findAppropriateSetterFor(Object obj) {
        JdbcPreparedStatementParamSetter setter;
        for ( int i = 0; i < this.settersQty; i++ ) {
            setter = this.setters.get(i);
            if ( setter.applicableTo(obj) ) {
                return setter;
            }
        }

        throw new JdbcPreparedStatementParamsException(
                "appropriate ParamsSetter not found for class: " +
                        obj.getClass().getCanonicalName());
    }
}
