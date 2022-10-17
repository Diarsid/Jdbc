package diarsid.jdbc.impl;

import diarsid.jdbc.api.sqltable.rows.RowOperation;
import diarsid.jdbc.api.sqltable.rows.collectors.RowsIterationAware;

import static diarsid.jdbc.api.sqltable.rows.collectors.RowsIterationAware.State.AFTER_ITERATING;
import static diarsid.jdbc.api.sqltable.rows.collectors.RowsIterationAware.State.BEFORE_ITERATION;

public class RowsIteration {

    public static void whenRowsIterationAwareDoBefore(RowOperation operation) {
        if (operation instanceof RowsIterationAware) {
            RowsIterationAware rowsAwareOperation = (RowsIterationAware) operation;
            rowsAwareOperation.beforeRows();
            rowsAwareOperation.setState(BEFORE_ITERATION);
        }
    }

    public static void whenRowsIterationAwareDoAfter(RowOperation operation) {
        if (operation instanceof RowsIterationAware) {
            RowsIterationAware rowsAwareOperation = (RowsIterationAware) operation;
            rowsAwareOperation.afterRows();
            rowsAwareOperation.setState(AFTER_ITERATING);
        }
    }

}
