/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.loader.app;
import com.dslab.loader.DBClient.DBClient;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Shiou
 */

public class ReadThread extends Thread{
  private final Random random; 
  private final Queue buffer; 
  private final String taskSql;
  private static int id = 0; // 蛋糕的流水號(所有廚師共通) 
  private final DBClient dbClient;
  private final Table tableInfo;
  
  public ReadThread(String name, Queue buffer, String taskSql, DBClient dbClient, Table tableInfo, long seed) { 
    super(name); 
    this.buffer = buffer; 
    this.random = new Random(seed);
    this.taskSql = taskSql;
    this.dbClient = dbClient;
    this.tableInfo = tableInfo;
  } 
  
  @Override
  public void run() {
  try { 
      Thread.sleep(random.nextInt(1000)); 
    try {
      List<String> content = dbClient.read(taskSql,tableInfo);
      Block block = new Block(content);
      buffer.put(block);
    } catch (SQLException ex) {
      Logger.getLogger(ReadThread.class.getName()).log(Level.SEVERE, null, ex);
    }
    //System.out.println(taskSql);
    //dbClient.close();
  } catch (InterruptedException e) { } { 
    } 
 } 
  private static synchronized int nextId() { 
    return id++; 
  } 
}
