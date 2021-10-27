package integrations.transactions;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import diarsid.jdbc.api.Jdbc;
import diarsid.jdbc.api.JdbcOption;
import diarsid.jdbc.api.JdbcTransaction;
import diarsid.jdbc.api.SqlConnectionsSource;
import diarsid.jdbc.api.ThreadBoundJdbcTransaction;
import diarsid.jdbc.api.TransactionAware;
import diarsid.jdbc.api.exceptions.ForbiddenTransactionOperation;
import diarsid.jdbc.api.exceptions.JdbcException;
import diarsid.jdbc.api.exceptions.TransactionTerminationException;
import diarsid.jdbc.api.sqltable.rows.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import testing.embedded.base.h2.H2TestDataBase;
import testing.embedded.base.h2.TestDataBase;

import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

import static diarsid.jdbc.api.Jdbc.WhenNoTransactionThen.IF_NO_TRANSACTION_OPEN_NEW;
import static diarsid.jdbc.api.JdbcOption.TRANSACTION_GUARD_ENABLED;
import static diarsid.jdbc.api.JdbcTransaction.State.CLOSED_COMMITTED;
import static diarsid.jdbc.api.JdbcTransaction.State.FAILED;
import static diarsid.jdbc.api.JdbcTransaction.State.OPEN;
import static diarsid.jdbc.api.JdbcTransaction.ThenDo.PROCEED;
import static diarsid.jdbc.api.JdbcTransaction.ThenDo.THROW;
import static diarsid.support.configuration.Configuration.configure;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 *
 * @author Diarsid
 */
public class JdbcTransactionTest {

    static {
        configure().withDefault();
    }
    
    private static final Logger logger = LoggerFactory.getLogger(JdbcTransactionTest.class);
    
    private static Jdbc JDBC;
    private static TestDataBase TEST_BASE;
    private static final String TABLE_1_CREATE = 
            "CREATE TABLE table_1 (" +
            "id     INTEGER         NOT NULL PRIMARY KEY," +
            "label  VARCHAR(100)    NOT NULL," +
            "index  INTEGER         NOT NULL," +
            "active BOOLEAN         NOT NULL)";
    private static final String TABLE_1_INSERT = 
            "INSERT INTO table_1 (id, label, index, active) " +
            "VALUES (?, ?, ?, ?)";
     
    private static final int row_1_id = 1;
    private static final int row_2_id = 2;
    private static final int row_3_id = 3;
    
    private static final String row_1_label = "name_1";
    private static final String row_2_label = "name_2";
    private static final String row_3_label = "name_3";
    
    private static final int row_1_index = 10;
    private static final int row_2_index = 20;
    private static final int row_3_index = 30;
    
    private static final boolean row_1_active = true;
    private static final boolean row_2_active = false;
    private static final boolean row_3_active = true;

    static class Model {

        final int id;
        final String label;
        final int index;
        final boolean active;

        public Model(int id, String label, int index, boolean active) {
            this.id = id;
            this.label = label;
            this.index = index;
            this.active = active;
        }
    }
    
    public JdbcTransactionTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        setupTestBase();
        setupTransactionsFactory();
        setupRequiredTables();    
    }
    
    @Before
    public void setUpCase() {
        setupTestData();
    }
    
    @After
    public void clearCase() {
        clearData();
    }
    
    private static void clearData() {
        try (Connection connection = TEST_BASE.getConnection();
                Statement st = connection.createStatement();) {
            st.executeUpdate("DELETE FROM table_1");
        } catch (SQLException e) {
            logger.error("test base data cleaning: ", e);
        }
    }

    private static void setupTestData() {
        try (Connection connection = TEST_BASE.getConnection();
                PreparedStatement st = connection.prepareStatement(TABLE_1_INSERT)) {
            st.setInt(1, row_1_id);
            st.setString(2, row_1_label);
            st.setInt(3, row_1_index);
            st.setBoolean(4, row_1_active);
            st.addBatch();
            st.setInt(1, row_2_id);
            st.setString(2, row_2_label);
            st.setInt(3, row_2_index);
            st.setBoolean(4, row_2_active);
            st.addBatch();
            st.setInt(1, row_3_id);
            st.setString(2, row_3_label);
            st.setInt(3, row_3_index);
            st.setBoolean(4, row_3_active);
            st.addBatch();
            
            int[] update = st.executeBatch();
        } catch (SQLException e) {
            logger.error("test base data prepopulation: ", e);
        }
    }

    private static TestDataBase setupTestBase() {
        TestDataBase testBase = new H2TestDataBase("transactions.test");
        TEST_BASE = testBase;
        return testBase;
    }
    
    private static void setupRequiredTables() {
        TEST_BASE.setupRequiredTable(TABLE_1_CREATE);
    }

    private static void setupTransactionsFactory() {
        Map<JdbcOption, Object> options = Map.of(TRANSACTION_GUARD_ENABLED, true);
        SqlConnectionsSource source = new SqlConnectionsSourceTestBase(TEST_BASE);
        JDBC = Jdbc.init(source, options);
    }
    
    static JdbcTransaction createTransaction() {
        JdbcTransaction jdbcTransaction = JDBC.createTransaction();
        return jdbcTransaction;
    }

    /**
     * Test of rollbackAndTerminate method, of class JdbcTransactionWrapper.
     */
    @Test
    public void testRollbackAndTerminate() throws Exception {
        try {
            int qtyBefore = TEST_BASE.countRowsInTable("table_1");
            assertEquals(3, qtyBefore);
            
            JdbcTransaction transaction = createTransaction();
            
            int update = transaction.doUpdate(
                "DELETE FROM table_1 WHERE label IS ? ",
                "name_2");
            
            assertEquals(1, update);
            
            transaction.rollbackAnd(THROW);
            
            fail();
            
        } catch (TransactionTerminationException transactionTerminationException) {
            assertTrue(true);
        }
        
        assertTrue(TEST_BASE.ifAllConnectionsReleased());
        
        int qtyAfter = TEST_BASE.countRowsInTable("table_1");
        assertEquals(3, qtyAfter);
    }

    /**
     * Test of doQuery method, of class JdbcTransactionWrapper.
     */
    @Test
    public void testDoQuery_3args_1() throws Exception {
        JdbcTransaction transaction = createTransaction();
        transaction.doQuery(
                (row) -> {
                    this.printDataFromRow(row, "multiple rows processing:");
                },
                "SELECT * FROM table_1");
        transaction.commitAndClose();
        
        assertTrue(TEST_BASE.ifAllConnectionsReleased());
    }

    /**
     * Test of doQuery method, of class JdbcTransactionWrapper.
     */
    @Test
    public void testDoQuery_3args_2() throws Exception {
        JdbcTransaction transaction = createTransaction();
        transaction.doQuery( 
                (row) -> {
                    this.printDataFromRow(row, "multiple rows processing:");
                },
                "SELECT * FROM table_1 WHERE label LIKE ? ",
                "ame_1");
        transaction.commitAndClose();
        
        assertTrue(TEST_BASE.ifAllConnectionsReleased());
    }

    private void printDataFromRow(Row row, String comment) {
        logger.info(
                format("[%s] id: %d, label: %s, index: %d, active: %s",
                       comment,
                       (int) row.get("id"),
                       (String) row.get("label"),
                       (int) row.get("index"),
                       (boolean) row.get("active")));
    }

    /**
     * Test of doQuery method, of class JdbcTransactionWrapper.
     */
    @Test
    public void testDoQuery_3args_3() throws Exception {
        
    }

    /**
     * Test of doQuery method, of class JdbcTransactionWrapper.
     */
    @Test
    public void testDoQuery_String_PerRowOperation() throws Exception {
    }

    /**
     * Test of doUpdate method, of class JdbcTransactionWrapper.
     */
    @Test
    public void testDoUpdate_String() throws Exception {
    }

    /**
     * Test of doUpdate method, of class JdbcTransactionWrapper.
     */
    @Test
    public void testDoUpdate_String_ObjectArr() throws Exception {
        int qtyBefore = TEST_BASE.countRowsInTable("table_1");
        assertEquals(3, qtyBefore);
        
        JdbcTransaction transaction = createTransaction();
        transaction.doUpdate(
                TABLE_1_INSERT, 
                4, "name_4", 40, false);        
        transaction.commitAndClose();
        
        assertTrue(TEST_BASE.ifAllConnectionsReleased());
        
        int qtyAfter = TEST_BASE.countRowsInTable("table_1");
        assertEquals(4, qtyAfter);
    }

    @Test
    public void testDoUpdate_String_ObjectArr_threadBoundedTransaction() throws Exception {
        int qtyBefore = TEST_BASE.countRowsInTable("table_1");
        assertEquals(3, qtyBefore);

        JDBC.doInTransaction(transaction -> {
            transaction.doUpdate(
                    TABLE_1_INSERT,
                    4, "name_4", 40, false);
        });

        assertTrue(TEST_BASE.ifAllConnectionsReleased());

        int qtyAfter = TEST_BASE.countRowsInTable("table_1");
        assertEquals(4, qtyAfter);
    }
    
    @Test(timeout = 3000)    
    public void testDoUpdate_String_ObjectArr_not_commited_should_be_teared_down_by_Guard() throws Exception {
        int qtyBefore = TEST_BASE.countRowsInTable("table_1");
        assertEquals(3, qtyBefore);
        
        JdbcTransaction transaction = createTransaction();
        transaction.doUpdate(
                TABLE_1_INSERT, 
                4, "name_4", 40, false);

        sleep(2500);

        // transaction has not been committed or rolled back properly.
        // it will be rolled back, restored and closed by JdbcTransactionGuardReal.
        
        assertTrue(TEST_BASE.ifAllConnectionsReleased());
        
        int qtyAfter = TEST_BASE.countRowsInTable("table_1");
        assertEquals(3, qtyAfter);
    }

    @Test()
    public void testDoUpdate_String_asList() throws Exception {
        int qtyBefore = TEST_BASE.countRowsInTable("table_1");
        assertEquals(3, qtyBefore);
        
        try {
            JdbcTransaction transaction = createTransaction();
            transaction.doBatchUpdate(
                    TABLE_1_INSERT, asList(
                            asList(4, "name_4", 40, false),
                            asList(5, "name_5", 50, false),
                            asList(6, "name_6", 60, false)));
            
            transaction.doBatchUpdate(
                    TABLE_1_INSERT, asList(
                            asList(7, "name_7", 740, false),
                            asList(8, "name_8", 70, false)));
            
            transaction.doUpdate(
                    TABLE_1_INSERT,
                    8, "name_7", 70, false); // <- SQLException should rise due to primary key violation
            
            fail();
        } catch (JdbcException e) {
            assertTrue(TEST_BASE.ifAllConnectionsReleased());
            int qtyAfter = TEST_BASE.countRowsInTable("table_1");
            assertEquals(3, qtyAfter);
        }
    }

    @Test()
    public void testDoUpdate_String_paramsMapper() throws Exception {
        int qtyBefore = TEST_BASE.countRowsInTable("table_1");
        assertEquals(3, qtyBefore);

        List<Model> models = List.of(
                new Model(4, "name_4", 40, false),
                new Model(5, "name_5", 50, false),
                new Model(6, "name_6", 60, false));

        try (var transaction = createTransaction()) {

            transaction.doBatchUpdate(
                    TABLE_1_INSERT,
                    (model, params) -> {
                        params.addNext(model.id);
                        params.addNext(model.label);
                        params.addNext(model.index);
                        params.addNext(model.active);
                    },
                    models);
        }

        assertTrue(TEST_BASE.ifAllConnectionsReleased());
        int qtyAfter = TEST_BASE.countRowsInTable("table_1");
        assertEquals(6, qtyAfter);
    }
    
    @Test()
    public void testDoUpdate_String_Params_success() throws Exception {
        int qtyBefore = TEST_BASE.countRowsInTable("table_1");
        assertEquals(3, qtyBefore);
        
        JdbcTransaction transaction = createTransaction();
        transaction.doBatchUpdate(
                TABLE_1_INSERT, asList(
                        asList(4, "name_4", 40, false),
                        asList(5, "name_5", 50, false),
                        asList(6, "name_6", 60, false)));

        transaction.doBatchUpdate(
                TABLE_1_INSERT, asList(
                        asList(7, "name_7", 740, false),
                        asList(8, "name_8", 70, false)));

        transaction.doUpdate(
                TABLE_1_INSERT,
                9, "name_7", 70, false); 
        
        transaction.commitAndClose();
            
        assertTrue(TEST_BASE.ifAllConnectionsReleased());
        int qtyAfter = TEST_BASE.countRowsInTable("table_1");
        assertEquals(9, qtyAfter);
    }

    /**
     * Test of doUpdate method, of class JdbcTransactionWrapper.
     */
    @Test
    public void testDoUpdate_String_List() throws Exception {
    }

    /**
     * Test of doBatchUpdate method, of class JdbcTransactionWrapper.
     */
    @Test
    public void testDoBatchUpdate_String_Set() throws Exception {
    }

    /**
     * Test of doBatchUpdate method, of class JdbcTransactionWrapper.
     */
    @Test
    public void testDoBatchUpdate_String_ParamsArr() throws Exception {
        int qtyBefore = TEST_BASE.countRowsInTable("table_1");
        assertEquals(3, qtyBefore);
        
        JdbcTransaction transaction = createTransaction();
        transaction.doBatchUpdate(
                TABLE_1_INSERT, asList(
                        asList(4, "name_4", 40, false),
                        asList(5, "name_5", 50, false),
                        asList(6, "name_6", 60, false)));        
        transaction.commitAndClose();
        
        assertTrue(TEST_BASE.ifAllConnectionsReleased());
        
        int qtyAfter = TEST_BASE.countRowsInTable("table_1");
        assertEquals(6, qtyAfter);
    }

    /**
     * Test of commit method, of class JdbcTransactionWrapper.
     */
    @Test
    public void testRollbackAndProceed() throws Exception {
        int qtyBefore = TEST_BASE.countRowsInTable("table_1");
        assertEquals(3, qtyBefore);
        
        JdbcTransaction transaction = createTransaction();
        transaction.doBatchUpdate(
                TABLE_1_INSERT, asList(
                        asList(4, "name_4", 40, false),
                        asList(5, "name_5", 50, false),
                        asList(6, "name_6", 60, false)));
        
        transaction.rollbackAnd(PROCEED);
        
        int qtyAfterRollback = TEST_BASE.countRowsInTable("table_1");
        assertEquals(3, qtyAfterRollback);
        
        transaction.doBatchUpdate(
                TABLE_1_INSERT,
                asList(4, "name_4", 40, false));
        transaction.commitAndClose();
        
        assertTrue(TEST_BASE.ifAllConnectionsReleased());
        
        int qtyAfter = TEST_BASE.countRowsInTable("table_1");
        assertEquals(4, qtyAfter);        
    }
    
    @Test
    public void testConditionals() throws Exception {
        int qtyBefore = TEST_BASE.countRowsInTable("table_1");
        assertEquals(3, qtyBefore);
        
        JdbcTransaction transaction = createTransaction();
                
        transaction
                .doBatchUpdate(
                        TABLE_1_INSERT, asList(
                                asList(4, "name_4", 40, false),
                                asList(5, "name_5", 50, false),
                                asList(6, "name_6", 60, false)));
        transaction.commitAndClose();
        
        assertTrue(TEST_BASE.ifAllConnectionsReleased());
        
        int qtyAfterTrueCondition = TEST_BASE.countRowsInTable("table_1");
        assertEquals(6, qtyAfterTrueCondition);
    }
    
    @Test
    public void testConditionals_ifTrue_stacked() throws Exception {
        int qtyBefore = TEST_BASE.countRowsInTable("table_1");
        assertEquals(3, qtyBefore);
        
        JdbcTransaction transaction = createTransaction();
                
        transaction
                .doBatchUpdate(
                        TABLE_1_INSERT, asList(
                                asList(4, "name_4", 40, false),
                                asList(5, "name_5", 50, false),
                                asList(6, "name_6", 60, false)));
        transaction.commitAndClose();
        
        assertTrue(TEST_BASE.ifAllConnectionsReleased());
        
        int qtyAfterTrueCondition = TEST_BASE.countRowsInTable("table_1");
        assertEquals(6, qtyAfterTrueCondition);
    }

    @Test
    public void testAutoCloseableTransaction() {
        int qtyBefore = TEST_BASE.countRowsInTable("table_1");
        assertEquals(3, qtyBefore);
        
        try ( JdbcTransaction transaction = createTransaction() ) {
            
            transaction
                    .doBatchUpdate(
                            TABLE_1_INSERT, asList(
                                    asList(4, "name_4", 40, false),
                                    asList(5, "name_5", 50, false),
                                    asList(6, "name_6", 60, false)));
            
            int count = transaction
                    .countQueryResults(
                            "SELECT * FROM table_1");
            
            assertEquals(6, count);
            
            //transaction.commit();   <- explicit commit() call is omitted!
        } catch (JdbcException e) {
            fail();
        } 
        
        assertTrue(TEST_BASE.ifAllConnectionsReleased());
        
        int qtyAfterTrueCondition = TEST_BASE.countRowsInTable("table_1");
        assertEquals(6, qtyAfterTrueCondition);
    }
    
    @Test
    public void streamedQueryTest() throws Exception {
        
        int qty = TEST_BASE.countRowsInTable("table_1");
        assertEquals(3, qty);

        List<String> list;
        try ( JdbcTransaction transaction = createTransaction() ) {
            list = transaction
                    .doQueryAndStream(
                            (row) -> {
                                return (int) row.get("index");
                            },
                            "SELECT * " +
                            "FROM table_1")
                    .filter(i -> i > 0)
                    .map(i -> String.valueOf(i) + ": index")
                    .collect(toList());
        }
        
        assertEquals(3, list.size());
        
        assertTrue(TEST_BASE.ifAllConnectionsReleased());
    }
    
    @Test
    public void streamedQueryTest_varargWrappedasList() throws Exception {
        
        int qty = TEST_BASE.countRowsInTable("table_1");
        assertEquals(3, qty);
        
        List<String> labelPatterns = asList("%na%", "%ame%", "%1%");

        List<String> list;
        try ( JdbcTransaction transaction = createTransaction() ) {
            list = transaction
                    .doQueryAndStream(
                            (row) -> {
                                return (int) row.get("index");
                            },
                            "SELECT * " +
                            "FROM table_1 " +
                            "WHERE  ( id IS ? ) AND " +
                            "       ( label LIKE ? ) AND ( label LIKE ? ) AND ( label LIKE ? ) " +
                            "       AND ( active IS ? ) ",
                            1, labelPatterns, true)
                    .map(i -> String.valueOf(i) + ": index")
                    .collect(toList());
        }

        assertEquals(1, list.size());
        
        assertTrue(TEST_BASE.ifAllConnectionsReleased());
    }
    
    @Test
    public void streamedQueryTest_vararg() throws Exception {
        
        int qty = TEST_BASE.countRowsInTable("table_1");
        assertEquals(3, qty);

        List<String> list;
        try ( JdbcTransaction transaction = createTransaction() ) {
            list = transaction
                    .doQueryAndStream(
                            (row) -> {
                                return (int) row.get("index");
                            },
                            "SELECT * " +
                                    "FROM table_1 " +
                                    "WHERE ( label LIKE ? ) AND ( label LIKE ? )",
                            "%m%", "%na%")
                    .map(i -> String.valueOf(i) + ": index")
                    .collect(toList());
        }

        assertEquals(3, list.size());
        
        assertTrue(TEST_BASE.ifAllConnectionsReleased());
    }
    
    @Test
    public void firstRowProcessTest() throws Exception {
        try ( JdbcTransaction transaction = createTransaction() ) {
            transaction
                    .doQueryAndProcessFirstRow(
                            (firstRow) -> {
                                this.printDataFromRow(firstRow, "first row processing");
                            },
                            "SELECT TOP 1 * " +
                            "FROM table_1 " +
                            "ORDER BY index ");
        }

        assertTrue(TEST_BASE.ifAllConnectionsReleased());
    }
    
    @Test
    public void firstRowConvertTest() throws Exception {
        String s;
        try (JdbcTransaction transaction = createTransaction()) {
            s = transaction
                    .doQueryAndConvertFirstRow(
                            (firstRow) -> {
                                return (String) firstRow.get("label");
                            },
                            "SELECT TOP 1 * " +
                            "FROM table_1 " +
                            "ORDER BY index "
                    )
                    .get();

            assertEquals(row_1_label, s);
        }

        assertTrue(TEST_BASE.ifAllConnectionsReleased());
    }
    
    // fix
    @Test
    public void firstRowConvertTest_empty() {
        clearData();
        Optional<String> s = Optional.empty();
        try (JdbcTransaction transact = createTransaction()) {
            s = transact
                    .doQueryAndConvertFirstRow(
                            (firstRow) -> {
                                // incorrect conversion causing NPE:
                                return String.valueOf(( int ) firstRow.get("index"));
                            },
                            "SELECT MIN(index) AS index " +
                            "FROM table_1 " +
                            "ORDER BY index ");
            fail();
        } catch (JdbcException e) {
            
        }
        
        assertTrue(TEST_BASE.ifAllConnectionsReleased());
        
        assertFalse(s.isPresent());
    }
    
    @Test
    public void directJdbcUsage() throws Exception {
        List<String> results = new ArrayList<>();
        try (JdbcTransaction transact = createTransaction()) {
            transact.useJdbcDirectly(
                    (connection) -> {
                        PreparedStatement ps = connection.prepareStatement(
                                "SELECT * FROM table_1 " +
                                "WHERE ( label LIKE ? ) AND ( id IS ? ) ");
                        ps.setString(1, "%ame%");
                        ps.setInt(2, 2);

                        ResultSet rs = ps.executeQuery();
                        while (rs.next()) {
                            results.add(rs.getString("label"));
                        }
                    });
        }

        assertEquals(1, results.size());
        
        assertTrue(TEST_BASE.ifAllConnectionsReleased());
    }
    
    @Test
    public void directJdbcUsage_use_forbidden_methods() throws Exception {
        List<String> results = new ArrayList<>();

        try (JdbcTransaction transact = createTransaction()) {
            transact
                    .useJdbcDirectly(
                            (connection) -> {
                        connection.setAutoCommit(true); // here
                        fail();
                        PreparedStatement ps = connection.prepareStatement(
                                "SELECT * FROM table_1 " +
                                "WHERE ( label LIKE ? ) AND ( id IS ? ) ");
                        ps.setString(1, "%ame%");
                        ps.setInt(2, 2);
                        
                        ResultSet rs = ps.executeQuery();
                        while ( rs.next() ) {
                            results.add(rs.getString("label"));
                        }
                    });
        } catch (JdbcException e) {
        }
        
        assertEquals(0, results.size());
        
        assertTrue(TEST_BASE.ifAllConnectionsReleased());
    }

    @Test
    public void transactionalProxyTest() {
        Supplier<String> jdbcSupplier = () -> {
            assertTrue(JDBC.threadBinding().isBound());
            return JDBC.threadBinding()
                    .currentTransaction()
                    .doQueryAndConvertFirstRow(
                            row -> row.stringOf("label"),
                            "SELECT label FROM table_1 WHERE id = ? ",
                            row_2_id)
                    .orElse("other");
        };

        Supplier<String> jdbcSupplierTx = JDBC.createTransactionalProxyFor(
                Supplier.class, jdbcSupplier, IF_NO_TRANSACTION_OPEN_NEW);

        String label = jdbcSupplierTx.get();

        assertEquals(label, row_2_label);
        assertTrue(TEST_BASE.ifAllConnectionsReleased());
    }

    @Test
    public void transactionalProxyTestNested() {
        AtomicReference<ThreadBoundJdbcTransaction> transactionRef = new AtomicReference<>();

        Supplier<String> jdbcSupplierNested = () -> {
            assertTrue(JDBC.threadBinding().isBound());

            transactionRef.set(JDBC.threadBinding().currentTransaction());

            return JDBC.threadBinding()
                    .currentTransaction()
                    .doQueryAndConvertFirstRow(
                            row -> row.stringOf("label"),
                            "SELECT label FROM table_1 WHERE id = ? ",
                            row_1_id)
                    .orElse("other");
        };

        Supplier<String> jdbcSupplierNestedTx = JDBC.createTransactionalProxyFor(
                Supplier.class, jdbcSupplierNested, IF_NO_TRANSACTION_OPEN_NEW);

        Supplier<String> jdbcSupplierOuter = () -> {
            assertTrue(JDBC.threadBinding().isBound());

            String s1 = jdbcSupplierNestedTx.get();

            assertEquals(
                    JDBC.threadBinding().currentTransaction(),
                    transactionRef.get());

            String s2 = JDBC.threadBinding()
                    .currentTransaction()
                    .doQueryAndConvertFirstRow(
                            row -> row.stringOf("label"),
                            "SELECT label FROM table_1 WHERE id = ? ",
                            row_2_id)
                    .orElse("other");

            return s1 + s2;
        };

        Supplier<String> jdbcSupplierOuterTx = JDBC.createTransactionalProxyFor(
                Supplier.class, jdbcSupplierOuter, IF_NO_TRANSACTION_OPEN_NEW);

        String result = jdbcSupplierOuterTx.get();

        assertEquals(result, row_1_label + row_2_label);
        assertTrue(TEST_BASE.ifAllConnectionsReleased());
    }

    @Test
    public void expectException() {
        Runnable jdbcRunnable = () -> {
            assertTrue(JDBC.threadBinding().isBound());
            JDBC.createTransaction();
            fail();
        };

        Runnable jdbcRunnableTx = JDBC.createTransactionalProxyFor(
                Runnable.class, jdbcRunnable, IF_NO_TRANSACTION_OPEN_NEW);

        try {
            jdbcRunnableTx.run();
        }
        catch (ForbiddenTransactionOperation e) {
            // success
        }
        catch (Throwable t) {
            fail();
        }

        assertTrue(TEST_BASE.ifAllConnectionsReleased());
    }

    @Test
    public void expectException_2() {
        Runnable jdbcRunnable1 = () -> {
            assertTrue(JDBC.threadBinding().isBound());
            throw new RuntimeException("logical exception");
        };

        AtomicInteger counter = new AtomicInteger();
        Runnable jdbcRunnable2 = () -> {
            assertTrue(JDBC.threadBinding().isBound());
            counter.incrementAndGet();
        };

        Runnable jdbcRunnable1Tx = JDBC.createTransactionalProxyFor(
                Runnable.class, jdbcRunnable1, IF_NO_TRANSACTION_OPEN_NEW);

        Runnable jdbcRunnable2Tx = JDBC.createTransactionalProxyFor(
                Runnable.class, jdbcRunnable2, IF_NO_TRANSACTION_OPEN_NEW);

        try {
            JDBC.doInTransaction(transaction -> {
                assertTrue(JDBC.threadBinding().isBound());
                try {
                    jdbcRunnable1Tx.run();
                    fail();
                }
                catch (RuntimeException e) {
                    // proceed
                }
                catch (Throwable t) {
                    fail();
                }

                assertTrue(JDBC.threadBinding().isBound());
                assertEquals(FAILED, JDBC.threadBinding().currentTransaction().state());
                jdbcRunnable2Tx.run();
            });
        }
        catch (ForbiddenTransactionOperation e) {
            // success
        }
        catch (Throwable t) {
            fail();
        }

        assertEquals(counter.get(), 0);
        assertTrue(TEST_BASE.ifAllConnectionsReleased());
    }

    static class AwareRunnable1 implements Runnable, TransactionAware {

        final List<String> actions = new ArrayList<>();

        @Override
        public void beforeTransactionOpenFor(Method method, Object[] args) {
            assertTrue(JDBC.threadBinding().isNotBound());
            actions.add("beforeOpen");
        }

        @Override
        public void run() {
            assertTrue(JDBC.threadBinding().isBound());
            assertThat(JDBC.threadBinding().currentTransaction().state(), equalTo(OPEN));
            actions.add("run");
        }

        @Override
        public void afterTransactionCommitAndCloseFor(Method method, Object[] args) {
            assertFalse(JDBC.threadBinding().isBound());
            assertTrue(TEST_BASE.ifAllConnectionsReleased());
            actions.add("afterCommitAndClose");
        }
    }

    @Test
    public void transactionAwareRunnableTest() {
        AwareRunnable1 runnable = new AwareRunnable1();

        Runnable runnableTx = JDBC.createTransactionalProxyFor(Runnable.class, runnable, IF_NO_TRANSACTION_OPEN_NEW);

        runnableTx.run();

        runnable.actions.forEach(System.out::println);

        assertThat(runnable.actions.size(), equalTo(3));
        assertThat(runnable.actions.get(0), equalTo("beforeOpen"));
        assertThat(runnable.actions.get(1), equalTo("run"));
        assertThat(runnable.actions.get(2), equalTo("afterCommitAndClose"));
    }

    static class AwareRunnable2 implements Runnable, TransactionAware {

        final List<String> actions = new ArrayList<>();

        @Override
        public void run() {
            assertTrue(JDBC.threadBinding().isBound());
            assertThat(JDBC.threadBinding().currentTransaction().state(), equalTo(OPEN));
            actions.add("run");
        }
    }

}
