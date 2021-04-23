/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package diarsid.jdbc.impl.conversion.sql2java;

/**
 *
 * @author Diarsid
 */
public interface SqlTypeToJavaTypeConversion {

    Class javaType();

    Class sqlType();
    
    default boolean matchBothTypes(Class sqlType, Class javaType) {
        return sqlType.equals(this.sqlType()) && javaType.equals(this.javaType());
    }
    
    Object convert(Object sqlObject);
}
