package diarsid.jdbc.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toList;

public class JdbcPreparedStatementSetter {
    
    private final Set<JdbcPreparedStatementParamSetter> setters;

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
        this.setters = unmodifiableSet(defaultSetters);
    }
    
    public void setParameters(PreparedStatement statement, Stream<Object> params) throws SQLException {
        int paramIndex = 1;
        for ( Object param : StreamsSupport.unwrap(params).collect(toList()) ) {
            this.findAppropriateSetterFor(param).setParameterInto(statement, paramIndex, param);
            paramIndex++;
        }
    }

    public void setParameters(PreparedStatement statement, Object... params) throws SQLException {
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
    }

    public void setParameters(PreparedStatement statement, List params) throws SQLException {
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
    }
    
    private JdbcPreparedStatementParamSetter findAppropriateSetterFor(Object obj) {
        return this.setters
                .stream()
                .filter(setter -> setter.applicableTo(obj))
                .findFirst()
                .orElseThrow(() -> 
                        new JdbcPreparedStatementParamsException(
                                "appropriate ParamsSetter not found for class: " + 
                                obj.getClass().getCanonicalName()));
    }
}
