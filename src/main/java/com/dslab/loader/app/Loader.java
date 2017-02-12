/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.loader.app;

import com.dslab.loader.DBClient.DBClient;
import com.dslab.loader.DBClient.MysqlDBClient;
import com.dslab.loader.DBClient.PhoenixDBClient;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Shiou
 */
public class Loader {
  
  private static final Log LOG = LogFactory.getLog(Loader.class);
  
  static List<TableTask> tableTask;
  
  public static void main(final String[] args) throws Exception {
    
    if (args.length != 3) {
      LOG.warn("Usage:bin/Loader <Loader's config path>");
      System.exit(0);
    }
    String configPath = args[2];
    Configs prop = new Configs(configPath);
    tableTask = new ArrayList<TableTask>();
    
    createTable(prop);
    load(prop);
  }
  
  public static void createTable(Configs prop) throws SQLException, ClassNotFoundException {
    DBClient sourceClient = new MysqlDBClient(prop.getInputURL());
    Iterator<String> tableList = sourceClient.getTableList(prop.getInputdb()).iterator();
    sourceClient.close();
    while(tableList.hasNext()) {
      String tableName = tableList.next();
      DBClient sourceClientRead = new MysqlDBClient(prop.getInputURL());
      sourceClientRead.setTable(tableName);
      Table tableInfo = new Table(tableName,sourceClientRead);
      TaskFactory task = new TaskFactory(sourceClientRead, prop.getTaskSize(), tableInfo);
      tableTask.add(task.getTaskList());
      sourceClientRead.close();
      /*DBClient targetClient = new PhoenixDBClient(prop.getOutputURL());
      targetClient.createTable(tableName, tableInfo);
      targetClient.close();*/
    }
      
  }
  
  public static void load(Configs prop) throws SQLException, ClassNotFoundException, InterruptedException {
    //String tableName = "boxes";
    /*DBClient sourceClient = new MysqlDBClient(prop.getInputURL());
    sourceClient.setTable(tableName);
    Table tableInfo = new Table(tableName,sourceClient);
    
    TaskFactory task = new TaskFactory(sourceClient, prop.getTaskSize(), tableInfo);
    tableTask.add(task.getTaskList());
    sourceClient.close();*/
    
    int readThread = prop.getReadThread();
    int writeThread = prop.getWriteThread();
    ExecutorService readExecutor = Executors.newFixedThreadPool(readThread);
    Buffer block = new Buffer(prop.getBlockNumber());
    
    Iterator<TableTask> tableTaskList = tableTask.iterator();
    while(tableTaskList.hasNext()){
      TableTask tableTaskNext = tableTaskList.next();
      //System.out.println(tableTaskNext.getTableInfo().getTableName());
      Iterator<String> taskSqlList = tableTaskNext.getTaskSqlList().iterator();
      while(taskSqlList.hasNext()) {
        ProducerThread producer = new ProducerThread(prop.getInputURL(),tableTaskNext,taskSqlList.next(),block);
        FutureTask readTask = new FutureTask(producer);
        readExecutor.submit(readTask);
        /*ConsumerThread consumer = new ConsumerThread(prop.getOutputURL(), block);
        consumer.consume(producer.produce());*/
      }      
    }
    ExecutorService writeExecutor = Executors.newFixedThreadPool(writeThread);
    for(int i=1; i <= writeThread; i++) {
      ConsumerThread consumer = new ConsumerThread(prop.getOutputURL(), block);
      FutureTask writeTask = new FutureTask(consumer);
      writeExecutor.submit(writeTask);
    }
    readExecutor.shutdown();
    //writeExecutor.shutdown();
    
/*
    //監聽生產者結果
    readExecutor.execute(new Runnable() {
      @Override
      public void run() {
        String st = readTask.get();
        System.out.println(st);
    });
    //監聽消費者結果
    executor.execute(new Runnable() {
      @Override
      public void run() {
        String st = writeTask.get();
        System.out.println(st);
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    });   
    */  
  }
}
