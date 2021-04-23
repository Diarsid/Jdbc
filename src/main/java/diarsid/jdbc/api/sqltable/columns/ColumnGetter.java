package diarsid.jdbc.api.sqltable.columns;

import java.time.LocalDateTime;
import java.util.UUID;

import diarsid.jdbc.api.sqltable.rows.RowGetter;

public interface ColumnGetter<T> extends RowGetter<T> {

    String columnName();

    static ColumnGetter<LocalDateTime> timeOf(String name) {
        return new ColumnGetterImpl<>(
                name,
                (columName, row) -> row.get(columName, LocalDateTime.class));
    }

    static ColumnGetter<UUID> uuidOf(String name) {
        return new ColumnGetterImpl<>(
                name,
                (columName, row) -> row.get(columName, UUID.class));
    }

    static ColumnGetter<Long> longOf(String name) {
        return new ColumnGetterImpl<>(
                name,
                (columName, row) -> row.get(columName, Long.class));
    }

    static ColumnGetter<Integer> intOf(String name) {
        return new ColumnGetterImpl<>(
                name,
                (columName, row) -> row.get(columName, Integer.class));
    }

    static ColumnGetter<Double> doubleOf(String name) {
        return new ColumnGetterImpl<>(
                name,
                (columName, row) -> row.get(columName, Double.class));
    }

    static ColumnGetter<Float> floatOf(String name) {
        return new ColumnGetterImpl<>(
                name,
                (columName, row) -> row.get(columName, Float.class));
    }

    static ColumnGetter<Boolean> booleanOf(String name) {
        return new ColumnGetterImpl<>(
                name,
                (columName, row) -> row.get(columName, Boolean.class));
    }

    static ColumnGetter<String> stringOf(String name) {
        return new ColumnGetterImpl<>(
                name,
                (columName, row) -> row.get(columName, String.class));
    }
}
