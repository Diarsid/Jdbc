/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package diarsid.jdbc.api;

import java.sql.Connection;
import java.sql.SQLException;

/**
 *
 * @author Diarsid
 */
public interface SqlConnectionsSource {
    
    Connection getConnection() throws SQLException;
    
    void close();
}
