/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package diarsid.jdbc.api.sqltable.rows;

/**
 *
 * @author Diarsid
 */

@FunctionalInterface
public interface RowOperation {
    
    void process(Row row);
}
