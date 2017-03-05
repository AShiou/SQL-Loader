
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dsclab.loader.loader;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


/**
 *
 * @author Shiou
 */
public class TableTask {
  private final Table tableInfo;
  private final LinkedList<String> taskSqlList;

  public TableTask (Table tableInfo, LinkedList<String> taskList) throws SQLException {
    this.tableInfo = tableInfo;
    this.taskSqlList = taskList;
  }
  
  public Table getTableInfo() {
    return tableInfo;
  }
  
  public LinkedList<String> getTaskSqlList() {
    return taskSqlList;
  }
}
