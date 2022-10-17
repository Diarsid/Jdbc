package diarsid.jdbc.api.sqltable.rows.collectors;

import diarsid.jdbc.api.sqltable.rows.RowOperation;

public interface RowsCollector extends RowOperation, RowsIterationAware {

    boolean isReusable();

    default RowsCollectorReusable asReusable() {
        return (RowsCollectorReusable) this;
    }

}
