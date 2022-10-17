package diarsid.jdbc.api.sqltable.columns;

import java.util.ArrayList;
import java.util.List;

import diarsid.jdbc.api.sqltable.rows.RowGetter;
import diarsid.jdbc.api.sqltable.rows.RowOperation;
import diarsid.jdbc.api.sqltable.rows.collectors.AbstractRowsCollector;
import diarsid.jdbc.api.sqltable.rows.collectors.RowsCollectorReusable;

import static diarsid.jdbc.api.sqltable.rows.collectors.RowsIterationAware.State.BEFORE_ITERATION;

class TableImpl extends AbstractRowsCollector implements Table.Columns {

    private static class TableRow implements Row {

        private static final int INITIAL = -1;

        private final TableImpl table;
        private int rowIndex;
        private int listIndex;

        public TableRow(TableImpl table) {
            this.table = table;
            this.rowIndex = INITIAL;
            this.listIndex = INITIAL;
        }

        private void checkListIndexOnGet(int rowIndex) {
            if ( rowIndex < 0 || rowIndex > this.table.iteratedRowsCount.get() - 1) {
                throw new IllegalArgumentException();
            }
        }

        @Override
        public Object get(String column) {
            int columnIndex = this.table.columns.indexOf(column);

            if ( columnIndex < 0 ) {
                throw new IllegalArgumentException();
            }

            int i = this.listIndex + columnIndex;
            return this.table.objects.get(i);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T get(String column, Class<T> t) {
            return (T) this.get(column);
        }

        @Override
        public byte[] getBytes(String column) {
            return (byte[]) this.get(column);
        }

        @Override
        public void index(int i) {
            this.rowIndex = this.table.checkRowIndex(i);
            this.listIndex = this.rowIndex * this.table.columns.size();
            this.rowIndex = i;
        }

        @Override
        public int index() {
            return this.rowIndex;
        }

        @Override
        public boolean hasNext() {
            return this.rowIndex < this.table.iteratedRowsCount.get() - 1;
        }

        @Override
        public boolean hasPrevious() {
            return this.rowIndex > 0;
        }

        @Override
        public void next() {
            int i = this.rowIndex + 1;
            this.table.checkRowIndex(i);
            this.rowIndex = i;
            this.listIndex = this.rowIndex * this.table.columns.size();
        }

        @Override
        public void previous() {
            if ( this.rowIndex == INITIAL ) {
                return;
            }

            int i = this.rowIndex - 1;
            if ( i < 0 ) {
                i = INITIAL;
            }
            this.rowIndex = i;
        }
    }

    private final List<String> columns;
    private final List<Object> objects;
    private final TableRow row;
    private boolean isClearable;

    TableImpl() {
        this.columns = new ArrayList<>();
        this.objects = new ArrayList<>();
        this.row = new TableRow(this);
        this.isClearable = true;
    }

    @Override
    public TableImpl add(String name) {
        this.columns.add(name);
        return this;
    }

    @Override
    public TableImpl clearable(boolean clearable) {
        this.isClearable = clearable;
        return this;
    }

    @Override
    public void process(diarsid.jdbc.api.sqltable.rows.Row row) {
        super.iteratedRowsCount.incrementAndGet();

        Object obj;
        String column;
        for (int i = 0; i < this.columns.size(); i++) {
            column = this.columns.get(i);
            try {
                obj = row.get(column);
                objects.add(obj);
            }
            catch (Exception e) {
                e.printStackTrace();
                objects.add(e);
            }
        }
    }

    @Override
    public boolean isReusable() {
        return this.isClearable;
    }

    @Override
    public RowsCollectorReusable asReusable() {
        if ( ! this.isClearable ) {
            throw new IllegalStateException();
        }

        return this;
    }

    @Override
    public void clear() {
        if ( this.isClearable ) {
            this.columns.clear();
            this.objects.clear();
            this.row.rowIndex = 0;
            super.state.resetTo(BEFORE_ITERATION);
            super.iteratedRowsCount.set(0);
        }
    }

    @Override
    public List<String> columns() {
        return this.columns;
    }

    @Override
    public void forEach(RowOperation rowOperation) {
        int index;
        for ( int row = 0; row < iteratedRowsCount.get(); row++ ) {
            index = row * this.columns.size();
            this.row.rowIndex = index;
            rowOperation.process(this.row);
        }
    }

    @Override
    public <T> List<T> fromEach(RowGetter<T> rowGetter) {
        List<T> list = new ArrayList<>();
        for ( int row = 0; row < iteratedRowsCount.get(); row++ ) {
            this.row.index(row);
            T t = rowGetter.getFrom(this.row);
            list.add(t);
        }

        return list;
    }

    @Override
    public Table.Row row() {
        return this.row;
    }

    private int checkRowIndex(int i) {
        if ( i < 0 ) {
            i = TableRow.INITIAL;
        }
        else if ( i > super.iteratedRowsCount.get() - 1 ) {
            throw new IllegalArgumentException();
        }
        return i;
    }
}
