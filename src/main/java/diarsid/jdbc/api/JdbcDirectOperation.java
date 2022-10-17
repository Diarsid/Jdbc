package diarsid.jdbc.api;

import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
public interface JdbcDirectOperation {
    
    void operateJdbcDirectly(Connection connection) throws SQLException;
}
