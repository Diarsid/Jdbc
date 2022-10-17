package diarsid.jdbc.api.sqltable.rows;

import java.util.function.Function;

@FunctionalInterface
public interface RowGetter<T> extends Function<Row, T> {
    
    T getFrom(Row row);

    @Override
    default T apply(Row row) {
        return this.getFrom(row);
    }
}
