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

/**
 *
 * @author Shiou
 */
public class Loader {
  public static void main(final String[] args) throws Exception {
    
    Configs prop = new Configs(args[2]);
    
    createTable(prop);
    
    //load(prop);
    /*DBClient sourceClient = new MysqlDBClient(prop.getInputURL());
    Iterator<String> tableList = sourceClient.getTableList(prop.getInputdb()).iterator();
    sourceClient.close();
    while(tableList.hasNext()) {
      //String tableName = tableList.next();
      String tableName = "boxes";
      //Create table
      DBClient sourceClient = new MysqlDBClient(prop.getInputURL());
      sourceClient.setTable(tableName);
      Table tableInfo = new Table(sourceClient);
      
      int readThread = prop.getReadThread();
      int writeThread = prop.getWriteThread();      
      AllTask task = new AllTask(sourceClient, readThread, tableName);
      sourceClient.close();
      System.out.println(tableName);

      DBClient targetClient = new PhoenixDBClient(prop.getOutputURL());
      targetClient.setTable(tableName);
      targetClient.createTable(tableName, tableInfo);
      targetClient.close();
      //Load data
      
    }*/
    
    /*Queue buffer = new Queue(prop.getBlockNumber());//建立可以放置n個block的buffer pool
    
    Iterator<String> taskSql = task.getTaskList().iterator();
    int tt=0;
    while(taskSql.hasNext()) {
      int num = 789;
      DBClient readClient = new MysqlDBClient(prop.getInputURL());
      readClient.setTable(tableName);
      new ReadThread("ReadThread", buffer, taskSql.next(), readClient, tableInfo, num).start();
      //List<String> qaq = readClient.read(taskSql.next(), tableInfo);
      //readClient.close();
      num++;
      tt++;
    }*/
    /*while(tt>0) {
      int qq=561;
      DBClient writeClient = new PhoenixDBClient(prop.getOutputURL());
      writeClient.setTable(tableName);
      new WriteThread("WriteThread", buffer, writeClient, qq).start();
        
        //writeClient.write(qaq);
        //writeClient.close();
        qq++;
        tt--;
    }*/
  }
  
  public static void createTable(Configs prop) throws SQLException, ClassNotFoundException {
    DBClient sourceClient = new MysqlDBClient(prop.getInputURL());
    Iterator<String> tableList = sourceClient.getTableList(prop.getInputdb()).iterator();
    sourceClient.close();
    while(tableList.hasNext()) {
      String tableName = tableList.next();
      DBClient sourceClientRead = new MysqlDBClient(prop.getInputURL());
      sourceClientRead.setTable(tableName);
      Table tableInfo = new Table(sourceClientRead);
      sourceClientRead.close();
      DBClient targetClient = new PhoenixDBClient(prop.getOutputURL());
      targetClient.setTable(tableName);
      targetClient.createTable(tableName, tableInfo);
      targetClient.close();
    }
      
  } 
  
}
