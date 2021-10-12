package diarsid.jdbc.api;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

public class Main {

    static class O {
        String string;
        int i;
        boolean b;

        public O(String string, int i, boolean b) {
            this.string = string;
            this.i = i;
            this.b = b;
        }
    }

    public static void main(String[] args) {
        List<O> os = asList(
                new O("one", 1, true),
                new O("two", 2, false),
                new O("three", 3, true)
        );

        JdbcOperations.ParamsFrom<O> oToArgs = o -> List.of(o.string, o.i, o.b);

        List objects = os
                .stream()
                .map(oToArgs)
                .collect(toList());
    }
}
