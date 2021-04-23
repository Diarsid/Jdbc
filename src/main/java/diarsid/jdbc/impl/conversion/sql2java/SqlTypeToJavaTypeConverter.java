/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package diarsid.jdbc.impl.conversion.sql2java;

import java.util.HashMap;
import java.util.Map;

import diarsid.jdbc.api.exceptions.JdbcException;

import static java.lang.String.format;
import static java.util.Objects.isNull;

/**
 *
 * @author Diarsid
 */
public class SqlTypeToJavaTypeConverter {
    
    private final Map<Class, Map<Class, SqlTypeToJavaTypeConversion>> conversions;

    public SqlTypeToJavaTypeConverter(SqlTypeToJavaTypeConversion... givenConversions) {
        this.conversions = new HashMap<>();

        for ( SqlTypeToJavaTypeConversion conversion : givenConversions ) {
            Map<Class, SqlTypeToJavaTypeConversion> nConversions = this.conversions.get(conversion.javaType());

            if ( isNull(nConversions) ) {
                nConversions = new HashMap<>();
                this.conversions.put(conversion.javaType(), nConversions);
            }

            nConversions.put(conversion.sqlType(), conversion);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T convert(Object obj, Class<T> type) {
        SqlTypeToJavaTypeConversion conversion = this.getConversion(obj.getClass(), type);

        return (T) conversion.convert(obj);
    }

    private SqlTypeToJavaTypeConversion getConversion(Class sqlType, Class javaType) {
        Map<Class, SqlTypeToJavaTypeConversion> sqlTypeConversions = this.conversions.get(javaType);

        if ( isNull(sqlTypeConversions) ) {
            String message = format(
                    "Can not convert Java type %s - no appropriate %s found!",
                    javaType.getCanonicalName(),
                    SqlTypeToJavaTypeConversion.class.getSimpleName());
            throw new JdbcException(message);
        }

        SqlTypeToJavaTypeConversion conversion = sqlTypeConversions.get(sqlType);

        if ( isNull(conversion) ) {
            String message = format(
                    "Can not convert SQL type %s - no appropriate %s found!",
                    sqlType.getCanonicalName(),
                    SqlTypeToJavaTypeConversion.class.getSimpleName());
            throw new JdbcException(message);
        }

        return conversion;
    }
    
}
