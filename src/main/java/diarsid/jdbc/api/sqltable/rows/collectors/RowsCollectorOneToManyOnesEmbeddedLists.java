package diarsid.jdbc.api.sqltable.rows.collectors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import diarsid.jdbc.api.sqltable.rows.Row;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class RowsCollectorOneToManyOnesEmbeddedLists<ONE, ONE_ID, MANY, MANY_ID> extends AbstractRowsCollector {

    private final List<ONE> ones;
    private final Map<ONE_ID, ONE> onesByIds;
    private final Map<MANY_ID, MANY> manysByIds;

    private final Function<ONE, List<MANY>> oneListGetter;
    private final Function<Row, ONE_ID> getIdOne;
    private final Function<Row, MANY_ID> getIdMany;
    private final Function<Row, ONE> getInstanceOne;
    private final Function<Row, MANY> getInstanceMany;

    public RowsCollectorOneToManyOnesEmbeddedLists(
            Function<ONE, List<MANY>> oneListGetter,
            Function<Row, ONE_ID> getIdOne,
            Function<Row, MANY_ID> getIdMany,
            Function<Row, ONE> getInstanceOne,
            Function<Row, MANY> getInstanceMany) {
        this.oneListGetter = oneListGetter;
        this.getIdOne = getIdOne;
        this.getIdMany = getIdMany;
        this.getInstanceOne = getInstanceOne;
        this.getInstanceMany = getInstanceMany;
        this.ones = new ArrayList<>();
        this.onesByIds = new HashMap<>();
        this.manysByIds = new HashMap<>();
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
                manysByIds.put(manyId, many);
            }

            List<MANY> manys;
            if ( isNull(one) ) {
                one = getInstanceOne.apply(row);
                this.onesByIds.put(oneId, one);

                manys = oneListGetter.apply(one);
                manys.add(many);

                this.ones.add(one);
                this.onesByIds.put(oneId, one);
            }
            else {
                manys = oneListGetter.apply(one);
                manys.add(many);
            }
        }
        else {
            if ( isNull(one) ) {
                one = getInstanceOne.apply(row);
                this.onesByIds.put(oneId, one);

                this.ones.add(one);
                this.onesByIds.put(oneId, one);
            }
        }
    }

    public List<ONE> ones() {
        return new ArrayList<>(this.ones);
    }

    @Override
    public void clear() {
        this.ones.clear();
        this.onesByIds.clear();
        this.manysByIds.clear();
    }
}
