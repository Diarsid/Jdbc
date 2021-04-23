/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package integrations.transactions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import diarsid.jdbc.api.Jdbc;
import diarsid.jdbc.api.JdbcOption;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import testing.embedded.base.h2.H2TestDataBase;
import testing.embedded.base.h2.TestDataBase;

import diarsid.jdbc.api.SqlConnectionsSource;
import diarsid.jdbc.api.JdbcTransaction;

import static java.util.stream.Collectors.toList;

import static diarsid.jdbc.api.JdbcOption.TRANSACTION_GUARD_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 * @author Diarsid
 */
public class SingleCaseExplorationTest {

    
    private static final Logger logger = LoggerFactory.getLogger(SingleCaseExplorationTest.class);
    
    private static Jdbc JDBC;
    private static TestDataBase TEST_BASE;
    private static final String TABLE_1_CREATE = 
            "CREATE TABLE table_2 (" +
            "id     INTEGER         NOT NULL PRIMARY KEY," +
            "label  VARCHAR(100)    NOT NULL," +
            "index  INTEGER         NOT NULL," +
            "active BOOLEAN         NOT NULL)";
    private static final String TABLE_1_INSERT = 
            "INSERT INTO table_2 (id, label, index, active) " +
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
    
    public SingleCaseExplorationTest() {
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
            st.executeUpdate("DELETE FROM table_2");
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
        TestDataBase testBase = new H2TestDataBase("transactions.test.single");
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
        return JDBC.createTransaction();
    }
    
    public void doBad() {
        throw new RuntimeException("test exception");
    }
    
    @Test
    public void single() {
       int qty = TEST_BASE.countRowsInTable("table_2");
        assertEquals(3, qty);
        
        try ( JdbcTransaction transact = JDBC.createTransaction() ) {
            List<String> list = transact
                    .doQueryAndStream(
                            (row) -> {
                                doBad();
                                return ( int ) row.get("index");
                            },
                            "SELECT * " +
                            "FROM table_2 " +
                            "WHERE ( label LIKE ? ) AND ( label LIKE ? )",
                            "%m%", "%na%")
                            .map(i -> String.valueOf(i) + ": index")
                            .collect(toList());
            fail();
            assertEquals(3, list.size());
        } catch (RuntimeException e) {
            // ok
        }
        
        assertTrue(TEST_BASE.ifAllConnectionsReleased());
    }
}
