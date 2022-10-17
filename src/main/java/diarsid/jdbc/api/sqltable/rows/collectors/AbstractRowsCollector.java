package diarsid.jdbc.api.sqltable.rows.collectors;

import java.util.concurrent.atomic.AtomicLong;

import diarsid.support.objects.references.Present;
import diarsid.support.objects.references.References;

import static diarsid.jdbc.api.sqltable.rows.collectors.RowsIterationAware.State.BEFORE_ITERATION;

public abstract class AbstractRowsCollector implements RowsCollectorReusable {

    protected final Present<RowsIterationAware.State> state;
    protected final AtomicLong iteratedRowsCount;

    public AbstractRowsCollector() {
        this.state = References.simplePresentOf(BEFORE_ITERATION);
        this.iteratedRowsCount = new AtomicLong(0);
    }

    @Override
    public final State state() {
        return this.state.get();
    }

    @Override
    public final void setState(State state) {
        this.state.resetTo(state);
    }

    @Override
    public void clear() {
        this.state.resetTo(BEFORE_ITERATION);
    }

    @Override
    public final long iteratedRowsCount() {
        return this.iteratedRowsCount.get();
    }
}
