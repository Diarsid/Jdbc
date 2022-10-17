package diarsid.jdbc.impl.conversion.sql2java;

public interface SqlTypeToJavaTypeConversion<JAVA, SQL> {

    Class<JAVA> javaType();

    Class<SQL> sqlType();
    
    default boolean matchBothTypes(Class<SQL> sqlType, Class<JAVA> javaType) {
        return sqlType.equals(this.sqlType()) && javaType.equals(this.javaType());
    }
    
    Object convert(Object sqlObject);
}
