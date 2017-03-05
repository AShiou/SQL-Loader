/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dsclab.loader.app;

import com.dsclab.loader.loader.Configs;
import com.dsclab.loader.loader.ConsumerThread;
import com.dsclab.loader.loader.DBClient;
import com.dsclab.loader.loader.Droptable;
import com.dsclab.loader.loader.MysqlDBClient;
import com.dsclab.loader.loader.PhoenixDBClient;
import com.dsclab.loader.loader.Produce;
import com.dsclab.loader.loader.ProducerThread;
import com.dsclab.loader.loader.Table;
import com.dsclab.loader.loader.TableTask;
import com.dsclab.loader.loader.TaskFactory;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Shiou
 */
public class Loader {
  
  private static final Log LOG = LogFactory.getLog(Loader.class);
  
  static LinkedList<TableTask> tableTask;
  
  public static void main(final String[] args) throws Exception {
    
    if (args.length != 4) {
      LOG.warn("Usage:bin/Loader <Loader's config path>");
      System.exit(0);
    }
    String configPath = args[3];
    Configs prop = new Configs(configPath);
    
    LOG.info("From MySql : " + prop.getInputURL());
    LOG.info("Task Size : " + prop.getTaskSize());
    
    tableTask = new LinkedList<TableTask>();
    Droptable dd = new Droptable();
    createTable(prop);
    load(prop);
  }
  
  public static void createTable(Configs prop) throws SQLException, ClassNotFoundException {
    LOG.info("Start create table");
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
      
      DBClient targetClient = new PhoenixDBClient(prop.getOutputURL());
      targetClient.createTable(tableName, tableInfo);
      targetClient.close();
    }
  }
  
  public static void load(Configs prop) throws SQLException, ClassNotFoundException, InterruptedException, ExecutionException {
    int readThread = prop.getReadThread();
    int writeThread = prop.getWriteThread();
    ExecutorService readExecutor = Executors.newFixedThreadPool(readThread);
    ExecutorService writeExecutor = Executors.newFixedThreadPool(writeThread);
    LOG.info("Start load: writeThread:" + writeThread
      + ", readThread:" + readThread);
    BlockingQueue<List<String>> contentQueue = new LinkedBlockingQueue<>();
    
    int tableCount = tableTask.size();
    int sum = 0;
    for(int i = 0 ; i < tableCount ; i++) {
      sum = sum + tableTask.get(i).getTaskSqlList().size();
    }
    for(int i = 0 ; i < sum ; i++) {
      readExecutor.submit(new ProducerThread(prop.getInputURL(),contentQueue));
      writeExecutor.submit(new ConsumerThread(prop.getOutputURL(), contentQueue));
    }

    readExecutor.shutdown();
    readExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    System.out.println("[CHIA7712] read threads end");
    writeExecutor.shutdown();
    writeExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    System.out.println("[CHIA7712] write threads end");
  }  
  
  public static Produce getProduce() {
    TableTask firstTableTask = tableTask.getFirst();
    if(tableTask.size() != 0){
      LinkedList<String> firstTaskSql = firstTableTask.getTaskSqlList();
      while(firstTaskSql.size() == 0) {
        tableTask.removeFirst();
        if(tableTask.size() == 0) {
          return null;
        }
        firstTableTask = tableTask.getFirst();
        firstTaskSql = firstTableTask.getTaskSqlList();
      }
      //firstTableTask = tableTask.getFirst();
      //firstTaskSql = firstTableTask.getTaskSqlList();
      Table firstTableInfo = firstTableTask.getTableInfo();
      System.out.println("Tablename:"+firstTableInfo.getTableName());
     
        System.out.println("next table:"+tableTask.getFirst().getTableInfo().getTableName());
        String taskSql = firstTaskSql.getFirst();
        firstTaskSql.removeFirst();
        System.out.println("TaskSql:"+taskSql);
        return new Produce(firstTableInfo , taskSql);
    }
    return null;
  }
  
  private static void waitAllDone(List<Future<String>> ths) throws InterruptedException, ExecutionException {
    for (Future<String> f : ths) {
      f.get();
    }
  }
  
}
