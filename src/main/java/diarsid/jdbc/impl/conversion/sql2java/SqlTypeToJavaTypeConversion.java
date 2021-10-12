package diarsid.jdbc.impl.conversion.sql2java;

public interface SqlTypeToJavaTypeConversion<J, S> {

    Class<J> javaType();

    Class<S> sqlType();
    
    default boolean matchBothTypes(Class<S> sqlType, Class<J> javaType) {
        return sqlType.equals(this.sqlType()) && javaType.equals(this.javaType());
    }
    
    Object convert(Object sqlObject);
}
