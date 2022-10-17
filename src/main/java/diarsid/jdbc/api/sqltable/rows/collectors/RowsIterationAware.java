package diarsid.jdbc.api.sqltable.rows.collectors;

import diarsid.support.objects.CommonEnum;

public interface RowsIterationAware {

    public static enum State implements CommonEnum<State> {

        BEFORE_ITERATION,
        ITERATING,
        AFTER_ITERATING
    }

    default void beforeRows() {
        // to override before rows iterations
    }

    default void afterRows() {
        // to override after rows iterated;
    }

    State state();

    void setState(State state);

    long iteratedRowsCount();
}
