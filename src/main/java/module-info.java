module diarsid.jdbc {

    requires java.sql;
    requires slf4j.api;
    requires diarsid.support;

    exports diarsid.jdbc.api;
    exports diarsid.jdbc.api.exceptions;
    exports diarsid.jdbc.api.sqltable.columns;
    exports diarsid.jdbc.api.sqltable.rows;
    exports diarsid.jdbc.api.sqltable.rows.collectors;
}
