package diarsid.jdbc.api;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import diarsid.jdbc.api.sqltable.rows.RowGetter;
import diarsid.jdbc.api.sqltable.rows.RowOperation;

public interface JdbcOperations {

    interface ParamsFrom<T> extends Function<T, List<Object>> {

        List<Object> paramsFrom(T t);

        default List<Object> apply(T t) {
            return this.paramsFrom(t);
        }

    }

    interface Params {

        Params addNext(Object param);

        int getNextParamIndex();

    }

    interface ParamsApplier<T> {

        void apply(T t, Params params);

    }

    void doQuery(
            RowOperation operation, String sql);

    void doQuery(
            RowOperation operation, String sql, List params);

    void doQuery(
            RowOperation operation, String sql, Object... params);

    void doQueryAndProcessFirstRow(
            RowOperation operation, String sql);

    void doQueryAndProcessFirstRow(
            RowOperation operation, String sql, Object... params);

    void doQueryAndProcessFirstRow(
            RowOperation operation, String sql, List params);

    <T> Stream<T> doQueryAndStream(
            RowGetter<T> conversion, String sql);

    <T> Stream<T> doQueryAndStream(
            RowGetter<T> conversion, String sql, List params);

    <T> Stream<T> doQueryAndStream(
            RowGetter<T> conversion, String sql, Object... params);

    <T> Optional<T> doQueryAndConvertFirstRow(
            RowGetter<T> conversion, String sql);

    <T> Optional<T> doQueryAndConvertFirstRow(
            RowGetter<T> conversion, String sql, Object... params);

    <T> Optional<T> doQueryAndConvertFirstRow(
            RowGetter<T> conversion, String sql, List params);

    int countQueryResults(
            String sql);

    int countQueryResults(
            String sql, Object... params);

    int countQueryResults(
            String sql, List params);

    int doUpdate(
            String updateSql);

    int doUpdate(
            String updateSql, Object... params);

    int doUpdate(
            String updateSql, List params);

    <T> int doUpdate(
            String updateSql, ParamsApplier<T> paramsApplier, T t);

    <K> List<K> doUpdateAndGetKeys(
            String updateSql, Class<K> keyType);

    <K> List<K> doUpdateAndGetKeys(
            String updateSql, Class<K> keyType, Object... params);

    <K> List<K> doUpdateAndGetKeys(
            String updateSql, Class<K> keyType, List params);

    int[] doBatchUpdate(
            String updateSql, List<List> batchParams);

    int[] doBatchUpdate(
            String updateSql, List... batchParams);

    <T> int[] doBatchUpdate(
            String updateSql, ParamsFrom<T> paramsFromT, List<T> tObjects);

    <T> int[] doBatchUpdate(
            String updateSql, ParamsFrom<T> paramsFromT, T... tObjects);

    <T> int[] doBatchUpdate(
            String updateSql, ParamsApplier<T> paramsApplier, List<T> tObjects);

    void useJdbcDirectly(JdbcDirectOperation jdbcOperation);

    static void mustAllBe(int expected, int[] batchedChanges) {
        for ( int change : batchedChanges ) {
            if ( change != expected ) {
                throw new IllegalStateException();
            }
        }
    }
}
