package diarsid.jdbc.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import diarsid.jdbc.api.JdbcOperations;
import diarsid.jdbc.api.exceptions.JdbcException;
import diarsid.support.objects.PooledReusable;
import diarsid.support.objects.references.Possible;

import static diarsid.support.objects.references.References.simplePossibleButEmpty;

public class ParamsToStatement extends PooledReusable implements JdbcOperations.Params {

    private static final int PREPARED_STATEMENT_FIRST_PARAM_INDEX = 1;

    private final JdbcPreparedStatementSetter paramsSetter;
    private final AtomicInteger index;
    private final Possible<PreparedStatement> statement;

    public ParamsToStatement(JdbcPreparedStatementSetter paramsSetter) {
        super();
        this.index = new AtomicInteger(PREPARED_STATEMENT_FIRST_PARAM_INDEX);
        this.paramsSetter = paramsSetter;
        this.statement = simplePossibleButEmpty();
    }

    @Override
    public JdbcOperations.Params addNext(Object param) {
        try {
            this.paramsSetter
                    .findAppropriateSetterFor(param)
                    .setParameterInto(
                            this.statement.orThrow(),
                            this.index.getAndIncrement(),
                            param);

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

    @Override
    public int getNextParamIndex() {
        return this.index.get();
    }

    public void resetParamIndex() {
        this.index.set(PREPARED_STATEMENT_FIRST_PARAM_INDEX);
    }

    @Override
    protected void clearForReuse() {
        this.index.set(PREPARED_STATEMENT_FIRST_PARAM_INDEX);
        this.statement.nullify();
    }
}
