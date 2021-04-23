package diarsid.jdbc.api.sqltable.columns;

import java.util.function.BiFunction;

import diarsid.jdbc.api.sqltable.rows.Row;

class ColumnGetterImpl<T> implements ColumnGetter<T> {

    private final String name;
    private final BiFunction<String, Row, T> getter;

    ColumnGetterImpl(String name, BiFunction<String, Row, T> getter) {
        this.name = name;
        this.getter = getter;
    }

    @Override
    public T getFrom(Row row) {
        return getter.apply(name, row);
    }

    @Override
    public String columnName() {
        return name;
    }
}
