package diarsid.jdbc.api.exceptions;

import java.sql.SQLException;

public class JdbcException extends RuntimeException {
    
    public JdbcException(Exception e) {
        super(e);
    }

    public JdbcException(String msg) {
        super(msg);
    }

    /**
     * Returns TRUE if this exception has been caused by a statement that
     * has violated the SQL table primary key constraint.
     *
     * @return  TRUE if this exception has been caused by a statement that
     *          has violated the SQL table primary key constraint.
     */
    public boolean causedByPrimaryKeyViolation() {
        return ((SQLException) this.getCause())
                .getSQLState()
                .startsWith("23");
    }
}
