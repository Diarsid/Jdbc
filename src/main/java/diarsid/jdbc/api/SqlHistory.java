package diarsid.jdbc.api;

import java.util.List;
import java.util.function.Consumer;

import diarsid.support.objects.CommonEnum;

import static diarsid.jdbc.api.SqlHistory.Query.ArgsType.NONE;

public interface SqlHistory {

    interface Record {

        enum Type implements CommonEnum<Type> {
            QUERY,
            ROLLBACK,
            COMMENT,
            EXCEPTION
        }

        String string();

        int index();

        Long millis();

        SqlHistory.Record.Type type();

    }

    interface Query extends Record {

        enum ArgsType implements CommonEnum<ArgsType> {
            LIST,
            ARRAY,
            NONE
        }

        boolean isBatch();

        default boolean hasArgs() {
            return this.argsType().equalTo(NONE);
        }

        default boolean hasNoArgs() {
            return this.argsType().notEqualTo(NONE);
        }

        ArgsType argsType();

        Object[] argsAsArray();

        List argsAsList();

    }

    int recordsTotal();

    long millisTotal();

    Record recordOf(int index);

    String reportAll();

    String reportLast();

    void reported();

    boolean hasUnreported();

    void forEach(Consumer<Record> recordConsumer);
}
