package diarsid.jdbc.impl;

import java.io.Closeable;
import java.io.IOException;

// this class is intended for return it from param setting
// inside try-with-resources
public class CloseableStub implements Closeable {

    public static final CloseableStub INSTANCE = new CloseableStub();

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
