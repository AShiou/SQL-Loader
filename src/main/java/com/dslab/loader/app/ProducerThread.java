/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.loader.app;

import com.dslab.loader.DBClient.DBClient;
import com.dslab.loader.DBClient.MysqlDBClient;
import com.dslab.loader.DBClient.PhoenixDBClient;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import static javafx.scene.input.KeyCode.V;

/**
 *
 * @author user
 */
public class ProducerThread implements Callable {
  
  private final String inputUrl;
  private final TableTask tableTask;
  private final String taskSql;
  private final Buffer block;
  
  public ProducerThread(String inputUrl, TableTask tableTask, String taskSql, Buffer block) {
    this.inputUrl = inputUrl;
    this.tableTask = tableTask;
    this.taskSql = taskSql;
    this.block = block;
  }
  
  public List<String> produce() throws SQLException, ClassNotFoundException {
    DBClient sourceClient = new MysqlDBClient(inputUrl);
    sourceClient.setTable(tableTask.getTableInfo().getTableName());
    List<String> content = sourceClient.read(taskSql, tableTask.getTableInfo());
    Iterator<String> contentList = content.iterator();
    while(contentList.hasNext()) {
      System.out.println(contentList.next());
    }
    sourceClient.close();
    
    return content;
  }

  @Override
  public Object call() throws Exception {
    block.put(produce());
    return Thread.currentThread().getName() + "结束";
  }
  
}
