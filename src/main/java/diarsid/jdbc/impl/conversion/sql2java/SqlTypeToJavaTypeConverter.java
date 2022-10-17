package diarsid.jdbc.impl.conversion.sql2java;

import java.util.HashMap;
import java.util.Map;

import diarsid.jdbc.api.exceptions.JdbcException;

import static java.lang.String.format;
import static java.util.Objects.isNull;

public class SqlTypeToJavaTypeConverter {

    @SuppressWarnings("rawtypes")
    private final Map<Class, Map<Class, SqlTypeToJavaTypeConversion>> conversions;

    @SuppressWarnings("rawtypes")
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <JAVA> JAVA convert(Object obj, Class<JAVA> javaType) {
        SqlTypeToJavaTypeConversion conversion = this.getConversion(obj.getClass(), javaType);

        return (JAVA) conversion.convert(obj);
    }

    @SuppressWarnings("rawtypes")
    public <SQL, JAVA> SqlTypeToJavaTypeConversion getConversion(Class<SQL> sqlType, Class<JAVA> javaType) {
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
