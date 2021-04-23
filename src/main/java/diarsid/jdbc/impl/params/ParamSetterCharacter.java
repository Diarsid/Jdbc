package diarsid.jdbc.impl.params;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import diarsid.jdbc.impl.JdbcPreparedStatementParamSetter;

public class ParamSetterCharacter implements JdbcPreparedStatementParamSetter {

    @Override
    public boolean applicableTo(Object o) {
        return o instanceof Character;
    }

    @Override
    public void setParameterInto(PreparedStatement preparedStatement, int i, Object o) throws SQLException {
        preparedStatement.setString(i, String.valueOf((Character) o));
    }
}
