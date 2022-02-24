package diarsid.jdbc.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import diarsid.jdbc.api.JdbcOperations;
import diarsid.jdbc.api.exceptions.JdbcException;
import diarsid.support.objects.PooledReusable;
import diarsid.support.objects.references.Possible;

import static diarsid.support.objects.references.References.simplePossibleButEmpty;

public class StatementParams extends PooledReusable implements JdbcOperations.Params {

    private static final int PREPARED_STATEMENT_FIRST_PARAM_INDEX = 1;

    private final JdbcPreparedStatementSetter paramsSetter;
    private final AtomicInteger index;
    private final List<Object> interceptedParams;
    private final Possible<PreparedStatement> statement;

    public StatementParams(JdbcPreparedStatementSetter paramsSetter) {
        super();
        this.paramsSetter = paramsSetter;
        this.index = new AtomicInteger(PREPARED_STATEMENT_FIRST_PARAM_INDEX);
        this.interceptedParams = new ArrayList<>();
        this.statement = simplePossibleButEmpty();
    }

    @Override
    public JdbcOperations.Params addNext(Object param) {
        PreparedStatement statement = this.statement.orThrow();
        try {
            this.paramsSetter
                    .findAppropriateSetterFor(param)
                    .setParameterInto(
                            statement,
                            this.index.getAndIncrement(),
                            param);

            this.interceptedParams.add(param);

            return this;
        }
        catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    public void useWith(PreparedStatement ps) {
        this.index.set(1);
        this.statement.resetTo(ps);
    }

    public List<Object> interceptedOnLastIteration() {
        return interceptedParams;
    }

    @Override
    public int getNextParamIndex() {
        return this.index.get();
    }

    public void reset() {
        this.index.set(PREPARED_STATEMENT_FIRST_PARAM_INDEX);
        this.interceptedParams.clear();
    }

    @Override
    protected void clearForReuse() {
        this.reset();
        this.statement.nullify();
    }
}
