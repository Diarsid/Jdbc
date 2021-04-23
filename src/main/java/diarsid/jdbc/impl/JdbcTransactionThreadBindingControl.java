package diarsid.jdbc.impl;

import diarsid.jdbc.api.JdbcTransactionThreadBinding;

public interface JdbcTransactionThreadBindingControl extends JdbcTransactionThreadBinding {

    void unbind();

    void bindNew();
}
