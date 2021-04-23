/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package diarsid.jdbc.impl.conversion.sql2java;


public class SqlDoubleToJavaFloatConversion implements SqlTypeToJavaTypeConversion {

    @Override
    public Class<Float> javaType() {
        return Float.class;
    }

    @Override
    public Class sqlType() {
        return Double.class;
    }

    @Override
    public Float convert(Object sqlObject) {
        return ((Double) sqlObject).floatValue();
    }
    
}
