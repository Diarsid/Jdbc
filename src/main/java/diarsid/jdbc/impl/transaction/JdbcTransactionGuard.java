/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package diarsid.jdbc.impl.transaction;

import java.util.concurrent.TimeUnit;

/**
 *
 * @author Diarsid
 */
public interface JdbcTransactionGuard {

    Runnable accept(JdbcTransactionReal tx);

    Runnable accept(JdbcTransactionReal tx, int timeout,  TimeUnit unit);

    void stop();
    
}
