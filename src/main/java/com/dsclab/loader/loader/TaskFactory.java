/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dsclab.loader.loader;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.lang.Math.*;
import java.util.LinkedList;
/**
 *
 * @author dslab
 */
public class TaskFactory {
  private final int rowCount;
  private final int taskNum;
  private final int taskSize;
  private final Table tableInfo;
  private final String tableName;
  
  public TaskFactory (DBClient dbconnect, int taskSize, Table tableInfo) throws SQLException {
    this.rowCount = dbconnect.getSqlCount();
    this.taskSize = taskSize;
    int taskNumInt = rowCount/taskSize;
    if(rowCount-taskSize*taskNumInt > 0)
      this.taskNum = taskNumInt+1;
    else
      this.taskNum = taskNumInt;
    this.tableInfo = tableInfo;
    this.tableName = tableInfo.getTableName();
  }
  
  public LinkedList<String> distributeTask() {
    int offset = 0;
    int rowSum = rowCount;
    LinkedList<String> list = new LinkedList<>();
    for(int i=1 ; i<= taskNum ; i++) {
      StringBuilder Gram = new StringBuilder();
      Gram.append("SELECT * FROM ");
      Gram.append(tableName);
      Gram.append(" order by 1 limit ");
      Gram.append(offset);
      Gram.append(",");
      if(rowSum < taskSize)
        Gram.append(rowSum);
      else
        Gram.append(taskSize);
      offset = offset + taskSize;
      rowSum = rowSum - taskSize;
      list.add(Gram.toString());
      //select * from XML3_devices_history order by 1 limit 189699
    }
    return list;
  }
  
  public TableTask getTaskList() throws SQLException {
    return new TableTask(tableInfo,distributeTask());
  }
}
