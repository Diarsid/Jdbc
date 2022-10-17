package diarsid.jdbc.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

public interface JdbcPreparedStatementParamSetter {

    boolean applicableTo(Object o);

    void setParameterInto(
            PreparedStatement statement, int index, Object param) 
            throws SQLException;

    default void setParameterAndIncrementIndexInto(
            PreparedStatement statement, AtomicInteger index, Object param)
            throws SQLException {
        this.setParameterInto(statement, index.get(), param);
        index.incrementAndGet();
    }
}
