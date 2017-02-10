/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.loader.app;

import com.dslab.loader.DBClient.DBClient;
import java.sql.SQLException;
import java.util.List;

/**
 *
 * @author Shiou
 */
public class Table {
  private final String priKey;
  private final List<Schema> schema;
  
  public Table(DBClient sourceDB) throws SQLException {
    
    this.priKey = sourceDB.getPriKey();
    this.schema = sourceDB.getSchema();
  }
  
  public String getPriKey() {
    return priKey;
  }
  
  public List<Schema> getSchema() {
    
    return schema;
  }
}
