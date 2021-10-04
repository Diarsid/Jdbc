package diarsid.jdbc.impl;

import diarsid.jdbc.api.JdbcTransaction;
import diarsid.jdbc.api.JdbcTransactionThreadBinding;

public interface JdbcTransactionThreadBindingControl extends JdbcTransactionThreadBinding {

    void unbind();

    void bindNew();

    void bindExisting(JdbcTransaction transaction);
}
