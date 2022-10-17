package diarsid.jdbc.impl;

public interface JdbcPreparedStatementParamSetterByClass<T> extends JdbcPreparedStatementParamSetter {

    Class<T> type();

    @Override
    default boolean applicableTo(Object o) {
        return this.type().isAssignableFrom(o.getClass());
    }
}
