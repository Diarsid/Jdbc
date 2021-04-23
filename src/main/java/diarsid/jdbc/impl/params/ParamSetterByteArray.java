/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package diarsid.jdbc.impl.params;

import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import diarsid.jdbc.impl.JdbcPreparedStatementParamSetter;


public class ParamSetterByteArray implements JdbcPreparedStatementParamSetter {

    public ParamSetterByteArray() {
    }

    @Override
    public boolean applicableTo(Object o) {
        return ( o instanceof byte[]);
    }

    @Override
    public void setParameterInto(PreparedStatement statement, int index, Object param) 
            throws SQLException {
        Blob blob = statement.getConnection().createBlob();
        blob.setBytes(1, (byte[])param);
        statement.setBlob(index, blob);
    }
}
