package diarsid.jdbc.api.sqltable.rows;

@FunctionalInterface
public interface RowOperation {
    
    void process(Row row);
}
