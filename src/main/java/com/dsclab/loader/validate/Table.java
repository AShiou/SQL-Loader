/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dsclab.loader.validate;

import com.dsclab.loader.loader.*;
import java.sql.SQLException;
import java.util.List;

/**
 *
 * @author Shiou
 */
public class Table {
  private final String tableName;
  private final String priKey;
  private final List<Schema> schema;
  
  public Table(String tableName, DBClient sourceDB) throws SQLException {
    this.tableName = tableName;
    this.priKey = sourceDB.getPriKey();
    this.schema = sourceDB.getSchema();
  }
  
  public String getTableName() {
    return tableName;
  }
  
  public String getPriKey() {
    return priKey;
  }
  
  public List<Schema> getSchema() {
    return schema;
  }
}
