package diarsid.jdbc.impl;

import diarsid.jdbc.impl.conversion.sql2java.SqlTypeToJavaTypeConverter;
import diarsid.support.objects.GuardedPool;

public final class JdbcImplStaticResources {

    public final JdbcPreparedStatementSetter paramsSetter;
    public final SqlTypeToJavaTypeConverter sqlTypeToJavaTypeConverter;
    public final GuardedPool<StatementParams> paramsPool;

    public JdbcImplStaticResources(JdbcPreparedStatementSetter paramsSetter, SqlTypeToJavaTypeConverter sqlTypeToJavaTypeConverter) {
        this.paramsSetter = paramsSetter;
        this.sqlTypeToJavaTypeConverter = sqlTypeToJavaTypeConverter;
        this.paramsPool = new GuardedPool<>(() -> new StatementParams(paramsSetter));
    }
}
