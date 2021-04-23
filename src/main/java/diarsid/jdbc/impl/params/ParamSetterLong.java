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
public class ParamSetterLong implements JdbcPreparedStatementParamSetter {

    public ParamSetterLong() {
    }

    @Override
    public boolean applicableTo(Object o) {
        return ( o instanceof Long );
    }

    @Override
    public void setParameterInto(PreparedStatement statement, int index, Object arg) 
            throws SQLException {
        statement.setLong(index, (long) arg);
    }
}
