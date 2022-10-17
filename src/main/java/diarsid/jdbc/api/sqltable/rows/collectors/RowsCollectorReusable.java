package diarsid.jdbc.api.sqltable.rows.collectors;

import diarsid.support.objects.StatefulClearable;

public interface RowsCollectorReusable extends RowsCollector, StatefulClearable {

    @Override
    default boolean isReusable() {
        return true;
    }
}
