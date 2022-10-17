package diarsid.jdbc.api.sqltable.columns;

import java.util.List;

import diarsid.jdbc.api.sqltable.rows.RowGetter;
import diarsid.jdbc.api.sqltable.rows.RowOperation;
import diarsid.jdbc.api.sqltable.rows.collectors.RowsCollectorReusable;

public interface Table extends RowsCollectorReusable {

    public interface Columns extends Table {

        public static Table.Columns of(String name) {
            TableImpl table = new TableImpl();
            table.add(name);
            return table;
        }

        public Table.Columns add(String name);

        public Table.Columns clearable(boolean clearable);
    }

    public interface Row extends diarsid.jdbc.api.sqltable.rows.Row {

        void index(int i);

        int index();

        boolean hasNext();

        boolean hasPrevious();

        void next();

        void previous();
    }

    public List<String> columns();

    public void forEach(RowOperation rowOperation);

    public <T> List<T> fromEach(RowGetter<T> rowGetter);

    public Table.Row row();

}
