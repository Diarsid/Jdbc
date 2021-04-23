package diarsid.jdbc.impl.conversion.sql2java;

import java.sql.Timestamp;
import java.time.LocalDateTime;

public class SqlTimestampToSqlLocalDateTimeConversion implements SqlTypeToJavaTypeConversion {

    @Override
    public Class javaType() {
        return LocalDateTime.class;
    }

    @Override
    public Class sqlType() {
        return Timestamp.class;
    }

    @Override
    public Object convert(Object sqlObject) {
        return ((Timestamp) sqlObject).toLocalDateTime();
    }
}
