/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package diarsid.jdbc.api.sqltable.rows;

import java.util.function.Function;

/**
 *
 * @author Diarsid
 */

@FunctionalInterface
public interface RowGetter<T> extends Function<Row, T> {
    
    T getFrom(Row row);

    @Override
    default T apply(Row row) {

        return this.getFrom(row);
    }
}
