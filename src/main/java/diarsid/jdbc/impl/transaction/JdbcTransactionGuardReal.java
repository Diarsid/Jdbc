/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package diarsid.jdbc.impl.transaction;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import diarsid.jdbc.api.exceptions.JdbcFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static diarsid.jdbc.api.JdbcTransaction.ThenDo.CLOSE;

/**
 *
 * @author Diarsid
 */
public class JdbcTransactionGuardReal implements JdbcTransactionGuard {
    
    private static final Logger logger = LoggerFactory.getLogger(JdbcTransactionGuardReal.class);
    
    private final ScheduledExecutorService scheduler;
    private final int transactionTimeout;

    public JdbcTransactionGuardReal(int notCommittedTransactionTeardownTimeout, ScheduledExecutorService scheduler) {
        this.transactionTimeout = notCommittedTransactionTeardownTimeout;
        this.scheduler  = scheduler;
    }
    
    @Override
    public Runnable accept(JdbcTransactionReal transaction) {
        Runnable delayedTearDown = this.delayedTearDownOf(transaction);
        ScheduledFuture scheduledTearDown = this.scheduler.schedule(
                delayedTearDown,
                this.transactionTimeout,
                MILLISECONDS);
        return () -> scheduledTearDown.cancel(true);
    }
    
    @Override
    public Runnable accept(JdbcTransactionReal transaction, int timeout,  TimeUnit unit) {
        Runnable delayedTearDown = this.delayedTearDownOf(transaction);
        ScheduledFuture scheduledTearDown = this.scheduler.schedule(delayedTearDown, timeout, unit);
        return () -> scheduledTearDown.cancel(true);
    }
    
    private Runnable delayedTearDownOf(JdbcTransactionReal transaction) {
        return () -> {
            try {
                if ( transaction.state().isOpen() ) {
                    logger.error("Transaction has not been committed or rolled back properly.");
                    transaction.rollbackAnd(CLOSE);
                    logger.error("Transaction has been rolled back and closed by JdbcTransactionGuard!");
                    logger.error(transaction.sqlHistory().reportAll());
                }
            } catch (Exception ex) {
                logger.error("cannot teardown connection: ", ex);
                throw new JdbcFailureException(
                        format("%s cannot tear down JDBC Connection.", 
                               this.getClass().getCanonicalName()));
            }
        };
    }
    
    @Override
    public void stop() {
        this.scheduler.shutdown();
        logger.info("stopped.");
    }
}
