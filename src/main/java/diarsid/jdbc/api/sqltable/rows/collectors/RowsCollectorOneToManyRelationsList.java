package diarsid.jdbc.api.sqltable.rows.collectors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import diarsid.jdbc.api.sqltable.rows.Row;
import diarsid.support.functional.TripleFunction;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class RowsCollectorOneToManyRelationsList<RELATION, ONE, ONE_ID, MANY, MANY_ID> extends AbstractRowsCollector {

    private final Map<ONE_ID, ONE> onesByIds;
    private final Map<MANY_ID, MANY> manysByIds;

    private final List<RELATION> relations;

    private final Function<Row, ONE_ID> getIdOne;
    private final Function<Row, MANY_ID> getIdMany;
    private final Function<Row, ONE> getInstanceOne;
    private final Function<Row, MANY> getInstanceMany;
    private final TripleFunction<ONE, MANY, Row, RELATION> oneManyAndRowToRelation;

    public RowsCollectorOneToManyRelationsList(
            Function<Row, ONE_ID> getIdOne,
            Function<Row, MANY_ID> getIdMany,
            Function<Row, ONE> getInstanceOne,
            Function<Row, MANY> getInstanceMany,
            TripleFunction<ONE, MANY, Row, RELATION> oneManyAndRowToRelation) {
        this.getIdOne = getIdOne;
        this.getIdMany = getIdMany;
        this.getInstanceOne = getInstanceOne;
        this.getInstanceMany = getInstanceMany;
        this.oneManyAndRowToRelation = oneManyAndRowToRelation;

        this.relations = new ArrayList<>();
        this.onesByIds = new HashMap<>();
        this.manysByIds = new HashMap<>();
    }

    @Override
    public void process(Row row) {
        super.iteratedRowsCount.incrementAndGet();

        ONE_ID oneId = getIdOne.apply(row);

        ONE one = onesByIds.get(oneId);

        if ( isNull(one) ) {
            one = getInstanceOne.apply(row);
            onesByIds.put(oneId, one);
        }

        MANY_ID manyId = getIdMany.apply(row);

        if ( nonNull(manyId) ) {
            MANY many = manysByIds.get(manyId);

            if ( isNull(many) ) {
                many = getInstanceMany.apply(row);
                manysByIds.put(manyId, many);
            }

            RELATION relation = oneManyAndRowToRelation.apply(one, many, row);
            relations.add(relation);
        }
    }

    public List<RELATION> relations() {
        return new ArrayList<>(relations);
    }

    @Override
    public void clear() {
        this.relations.clear();
        this.onesByIds.clear();
        this.manysByIds.clear();
    }
}
