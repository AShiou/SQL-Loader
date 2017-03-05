/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dsclab.loader.loader;

/**
 *
 * @author dslab
 */
public class Produce {
  
  private final Table tableInfo;
  private final String taskSql;
  
  public Produce ( Table tableInfo, String taskSql) {
    this.tableInfo = tableInfo;
    this.taskSql = taskSql;
  }
  
  public Table getTableInfo() {
    return tableInfo;
  }
  
  public String getTaskSql() {
    return taskSql;
  }
}
