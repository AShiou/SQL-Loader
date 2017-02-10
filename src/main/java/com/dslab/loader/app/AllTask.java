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
 * @author dslab
 */
public class AllTask {
  static int rowCount;
  static int taskNum;
  static int taskSize;
  static String tableName;
  static List<String> taskList;
  
  public AllTask (DBClient dbconnect,int taskNum, String tableName) throws SQLException {
    this.rowCount = dbconnect.getSqlCount();
    this.taskNum = taskNum;
    this.taskSize = rowCount/taskNum;
    this.tableName = tableName;
    this.taskList = distributeTask();
  }
  public List<String> distributeTask() {
    int offset = 0;
    List<String> list = new ArrayList<>();
    for(int i=1 ; i<=taskNum ; i++) {
      StringBuilder Gram = new StringBuilder();
      Gram.append("SELECT * FROM ");
      Gram.append(tableName);
      Gram.append(" order by 1 limit ");
      Gram.append(offset);
      Gram.append(",");
      Gram.append(taskSize);
      offset = offset+ taskSize;
      
      list.add(Gram.toString());
      //select * from XML3_devices_history order by 1 limit 189699
      //結尾不足先掠過
    }
    return list;
  }
  
  public List<String> getTaskList() {
    return taskList;
  }
  
}
