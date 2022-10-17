package diarsid.jdbc.api.sqltable.rows.collectors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import diarsid.jdbc.api.sqltable.rows.Row;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class RowsCollectorOneToManyMap<ONE, ONE_ID, MANY, MANY_ID> extends AbstractRowsCollector {

    private final Map<ONE_ID, ONE> onesByIds;
    private final Map<MANY_ID, MANY> manysByIds;
    private final Map<ONE, List<MANY>> manyByOne;

    private final Function<Row, ONE_ID> getIdOne;
    private final Function<Row, MANY_ID> getIdMany;
    private final Function<Row, ONE> getInstanceOne;
    private final Function<Row, MANY> getInstanceMany;

    public RowsCollectorOneToManyMap(
            Function<Row, ONE_ID> getIdOne,
            Function<Row, MANY_ID> getIdMany,
            Function<Row, ONE> getInstanceOne,
            Function<Row, MANY> getInstanceMany) {
        this.getIdOne = getIdOne;
        this.getIdMany = getIdMany;
        this.getInstanceOne = getInstanceOne;
        this.getInstanceMany = getInstanceMany;
        this.onesByIds = new HashMap<>();
        this.manysByIds = new HashMap<>();
        this.manyByOne = new HashMap<>();
    }

    @Override
    public void process(Row row) {
        super.iteratedRowsCount.incrementAndGet();

        ONE_ID oneId = getIdOne.apply(row);
        MANY_ID manyId = getIdMany.apply(row);

        ONE one = onesByIds.get(oneId);
        if ( nonNull(manyId) ) {
            MANY many = manysByIds.get(manyId);

            if ( isNull(many) ) {
                many = getInstanceMany.apply(row);
                if ( isNull(many) ) {

                }
                manysByIds.put(manyId, many);
            }

            List<MANY> manies;
            if ( isNull(one) ) {
                one = getInstanceOne.apply(row);
                onesByIds.put(oneId, one);

                manies = new ArrayList<>();
                manies.add(many);

                manyByOne.put(one, manies);
            }
            else {
                manies = manyByOne.get(one);
                manies.add(many);
            }
        }
        else {
            if ( isNull(one) ) {
                one = getInstanceOne.apply(row);
                onesByIds.put(oneId, one);
            }
        }
    }

    public Map<ONE, List<MANY>> oneToMany() {
        return this.manyByOne;
    }

    @Override
    public void clear() {
        this.onesByIds.clear();
        this.manysByIds.clear();
        this.manyByOne.clear();
    }
}
