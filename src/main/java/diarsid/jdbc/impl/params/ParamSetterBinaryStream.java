/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package diarsid.jdbc.impl.params;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import diarsid.jdbc.impl.JdbcPreparedStatementParamSetter;


public class ParamSetterBinaryStream implements JdbcPreparedStatementParamSetter {

    public ParamSetterBinaryStream() {
    }

    @Override
    public boolean applicableTo(Object o) {
        return ( o instanceof InputStream );
    }

    @Override
    public void setParameterInto(PreparedStatement statement, int index, Object param) 
            throws SQLException {
        statement.setBinaryStream(index, ((InputStream) param));
    }
}
