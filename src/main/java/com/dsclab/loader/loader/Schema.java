/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dsclab.loader.loader;

/**
 *
 * @author Shiou
 */

public class Schema {
  public String colType;
  public String colName;
  
  public Schema(int jdbcType,String colName) {
    ColType phoenixType = new ColType(jdbcType);
    this.colType = phoenixType.getType();
    this.colName = colName;
  }
  
  public String getColType() {
    return colType;
  }
  
  public String getColName() {
    return colName;
  }
}
