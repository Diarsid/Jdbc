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

import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toList;

public class JdbcPreparedStatementSetter {

    private final List<JdbcPreparedStatementParamSetter> setters;
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
    }
    
    public Closeable setParameters(PreparedStatement statement, Stream<Object> params) throws SQLException {
        int paramIndex = 1;
        for ( Object param : StreamsSupport.unwrap(params).collect(toList()) ) {
            this.findAppropriateSetterFor(param).setParameterInto(statement, paramIndex, param);
            paramIndex++;
        }

        return CloseableStub.INSTANCE;
    }

    public Closeable setParameters(PreparedStatement statement, Object param1) throws SQLException {
        AtomicInteger index = new AtomicInteger(1);

        this.setParameterUsingIncrementedIndex(statement, index, param1);

        return CloseableStub.INSTANCE;
    }

    public Closeable setParameters(PreparedStatement statement, Object param1, Object param2) throws SQLException {
        AtomicInteger index = new AtomicInteger(1);

        this.setParameterUsingIncrementedIndex(statement, index, param1);
        this.setParameterUsingIncrementedIndex(statement, index, param2);

        return CloseableStub.INSTANCE;
    }

    public Closeable setParameters(PreparedStatement statement, Object param1, Object param2, Object param3) throws SQLException {
        AtomicInteger index = new AtomicInteger(1);

        this.setParameterUsingIncrementedIndex(statement, index, param1);
        this.setParameterUsingIncrementedIndex(statement, index, param2);
        this.setParameterUsingIncrementedIndex(statement, index, param3);

        return CloseableStub.INSTANCE;
    }

    public Closeable setParameters(PreparedStatement statement, Object param1, Object param2, Object param3, Object param4) throws SQLException {
        AtomicInteger index = new AtomicInteger(1);

        this.setParameterUsingIncrementedIndex(statement, index, param1);
        this.setParameterUsingIncrementedIndex(statement, index, param2);
        this.setParameterUsingIncrementedIndex(statement, index, param3);
        this.setParameterUsingIncrementedIndex(statement, index, param4);

        return CloseableStub.INSTANCE;
    }

    public Closeable setParameters(PreparedStatement statement, Object param1, Object param2, Object param3, Object param4, Object param5) throws SQLException {
        AtomicInteger index = new AtomicInteger(1);

        this.setParameterUsingIncrementedIndex(statement, index, param1);
        this.setParameterUsingIncrementedIndex(statement, index, param2);
        this.setParameterUsingIncrementedIndex(statement, index, param3);
        this.setParameterUsingIncrementedIndex(statement, index, param4);
        this.setParameterUsingIncrementedIndex(statement, index, param5);

        return CloseableStub.INSTANCE;
    }

    public Closeable setParameters(PreparedStatement statement, Object param1, Object param2, Object param3, Object param4, Object param5, Object param6) throws SQLException {
        AtomicInteger index = new AtomicInteger(1);

        this.setParameterUsingIncrementedIndex(statement, index, param1);
        this.setParameterUsingIncrementedIndex(statement, index, param2);
        this.setParameterUsingIncrementedIndex(statement, index, param3);
        this.setParameterUsingIncrementedIndex(statement, index, param4);
        this.setParameterUsingIncrementedIndex(statement, index, param5);
        this.setParameterUsingIncrementedIndex(statement, index, param6);

        return CloseableStub.INSTANCE;
    }

    public Closeable setParameters(PreparedStatement statement, Object param1, Object param2, Object param3, Object param4, Object param5, Object param6, Object param7) throws SQLException {
        AtomicInteger index = new AtomicInteger(1);

        this.setParameterUsingIncrementedIndex(statement, index, param1);
        this.setParameterUsingIncrementedIndex(statement, index, param2);
        this.setParameterUsingIncrementedIndex(statement, index, param3);
        this.setParameterUsingIncrementedIndex(statement, index, param4);
        this.setParameterUsingIncrementedIndex(statement, index, param5);
        this.setParameterUsingIncrementedIndex(statement, index, param6);
        this.setParameterUsingIncrementedIndex(statement, index, param7);

        return CloseableStub.INSTANCE;
    }

    public Closeable setParameters(PreparedStatement statement, Object param1, Object param2, Object param3, Object param4, Object param5, Object param6, Object param7, Object param8) throws SQLException {
        AtomicInteger index = new AtomicInteger(1);

        this.setParameterUsingIncrementedIndex(statement, index, param1);
        this.setParameterUsingIncrementedIndex(statement, index, param2);
        this.setParameterUsingIncrementedIndex(statement, index, param3);
        this.setParameterUsingIncrementedIndex(statement, index, param4);
        this.setParameterUsingIncrementedIndex(statement, index, param5);
        this.setParameterUsingIncrementedIndex(statement, index, param6);
        this.setParameterUsingIncrementedIndex(statement, index, param7);
        this.setParameterUsingIncrementedIndex(statement, index, param8);

        return CloseableStub.INSTANCE;
    }

    public Closeable setParameters(PreparedStatement statement, Object param1, Object param2, Object param3, Object param4, Object param5, Object param6, Object param7, Object param8, Object param9) throws SQLException {
        AtomicInteger index = new AtomicInteger(1);

        this.setParameterUsingIncrementedIndex(statement, index, param1);
        this.setParameterUsingIncrementedIndex(statement, index, param2);
        this.setParameterUsingIncrementedIndex(statement, index, param3);
        this.setParameterUsingIncrementedIndex(statement, index, param4);
        this.setParameterUsingIncrementedIndex(statement, index, param5);
        this.setParameterUsingIncrementedIndex(statement, index, param6);
        this.setParameterUsingIncrementedIndex(statement, index, param7);
        this.setParameterUsingIncrementedIndex(statement, index, param8);
        this.setParameterUsingIncrementedIndex(statement, index, param9);

        return CloseableStub.INSTANCE;
    }

    public Closeable setParameters(PreparedStatement statement, Object param1, Object param2, Object param3, Object param4, Object param5, Object param6, Object param7, Object param8, Object param9, Object param10) throws SQLException {
        AtomicInteger index = new AtomicInteger(1);

        this.setParameterUsingIncrementedIndex(statement, index, param1);
        this.setParameterUsingIncrementedIndex(statement, index, param2);
        this.setParameterUsingIncrementedIndex(statement, index, param3);
        this.setParameterUsingIncrementedIndex(statement, index, param4);
        this.setParameterUsingIncrementedIndex(statement, index, param5);
        this.setParameterUsingIncrementedIndex(statement, index, param6);
        this.setParameterUsingIncrementedIndex(statement, index, param7);
        this.setParameterUsingIncrementedIndex(statement, index, param8);
        this.setParameterUsingIncrementedIndex(statement, index, param9);
        this.setParameterUsingIncrementedIndex(statement, index, param10);

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
    
    public JdbcPreparedStatementParamSetter findAppropriateSetterFor(Object obj) {
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

    private Closeable setParameterUsingIncrementedIndex(
            PreparedStatement statement, AtomicInteger index, Object param)
            throws SQLException {
        if ( param instanceof Collection ) {
            Collection nestedParams = (Collection) param;
            for ( Object nestedParam : nestedParams ) {
                if ( nestedParam instanceof Collection || nestedParam instanceof Object[] ) {
                    throw new ForbiddenTransactionOperation("Params nested more than one level are not supported");
                }
                this.findAppropriateSetterFor(param).setParameterAndIncrementIndexInto(statement, index, nestedParam);
            }
        }
        else if ( param instanceof Object[] ) {
            Object[] nestedParams = (Object[]) param;
            for ( Object nestedParam : nestedParams ) {
                if ( nestedParam instanceof Collection || nestedParam instanceof Object[] ) {
                    throw new ForbiddenTransactionOperation("Params nested more than one level are not supported");
                }
                this.findAppropriateSetterFor(param).setParameterAndIncrementIndexInto(statement, index, nestedParam);
            }
        }
        else {
            this.findAppropriateSetterFor(param).setParameterAndIncrementIndexInto(statement, index, param);
        }
        return CloseableStub.INSTANCE;
    }
}
