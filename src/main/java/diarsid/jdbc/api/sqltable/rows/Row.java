package diarsid.jdbc.api.sqltable.rows;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

public interface Row {
    
    Object get(String columnLabel);
    
    <T> T get(String columnLabel, Class<T> t);
    
    byte[] getBytes(String columnLabel);

    default LocalDate dateOf(String name) {
        return this.get(name, LocalDate.class);
    }

    default LocalDateTime timeOf(String name) {
        return this.get(name, LocalDateTime.class);
    }

    default UUID uuidOf(String name) {
        return this.get(name, UUID.class);
    }

    default Long longOf(String name) {
        return this.get(name, Long.class);
    }

    default Integer intOf(String name) {
        return this.get(name, Integer.class);
    }

    default Double doubleOf(String name) {
        return this.get(name, Double.class);
    }

    default Float floatOf(String name) {
        return this.get(name, Float.class);
    }

    default Boolean booleanOf(String name) {
        return this.get(name, Boolean.class);
    }

    default String stringOf(String name) {
        return this.get(name, String.class);
    }
}
