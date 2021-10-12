package diarsid.jdbc.impl;

import java.io.Closeable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface JdbcPreparedStatementParamSetter {

    boolean applicableTo(Object o);

    void setParameterInto(
            PreparedStatement statement, int index, Object param) 
            throws SQLException;    
}
