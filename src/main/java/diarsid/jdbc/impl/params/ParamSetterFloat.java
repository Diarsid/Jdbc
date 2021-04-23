/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package diarsid.jdbc.impl.params;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import diarsid.jdbc.impl.JdbcPreparedStatementParamSetter;

/**
 *
 * @author Diarsid
 */
public class ParamSetterFloat implements JdbcPreparedStatementParamSetter {

    public ParamSetterFloat() {
    }

    @Override
    public boolean applicableTo(Object o) {
        return ( o instanceof Float );
    }

    @Override
    public void setParameterInto(PreparedStatement statement, int index, Object arg) 
            throws SQLException {
        statement.setFloat(index, (float) arg);
    }
}
