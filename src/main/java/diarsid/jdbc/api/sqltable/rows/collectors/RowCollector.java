package diarsid.jdbc.api.sqltable.rows.collectors;

import diarsid.jdbc.api.sqltable.rows.RowOperation;

public interface RowCollector extends RowOperation {

    boolean isReusable();

    default RowCollectorReusable asReusable() {
        return (RowCollectorReusable) this;
    }

}
