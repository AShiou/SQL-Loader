/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.loader.app;

import com.dslab.loader.DBClient.DBClient;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


/**
 *
 * @author Shiou
 */
public class TableTask {
  private final Table tableInfo;
  private final List<String> taskSqlList;

  public TableTask (Table tableInfo, List<String> taskList) throws SQLException {
    this.tableInfo = tableInfo;
    this.taskSqlList = taskList;
  }
  
  public Table getTableInfo() {
    return tableInfo;
  }
  
  public List<String> getTaskSqlList() {
    return taskSqlList;
  }
}
