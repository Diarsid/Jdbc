/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package diarsid.jdbc.impl.params;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

import diarsid.jdbc.impl.JdbcPreparedStatementParamSetter;

/**
 *
 * @author Diarsid
 */
public class ParamSetterUUID implements JdbcPreparedStatementParamSetter {

    public ParamSetterUUID() {
    }
    
    @Override
    public boolean applicableTo(Object o) {
        return ( o instanceof UUID );
    }
    
    @Override
    public void setParameterInto(PreparedStatement statement, int index, Object arg) 
            throws SQLException {
        statement.setObject(index, (UUID) arg);
    }
}
