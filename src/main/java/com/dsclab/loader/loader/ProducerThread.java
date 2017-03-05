/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dsclab.loader.loader;

import static com.dsclab.loader.app.Loader.getProduce;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author user
 */
public class ProducerThread implements Callable<String> {
  private static final Log LOG = LogFactory.getLog(ProducerThread.class);
  public static long getIds() {
    return IDS.get();
  }
  public static long getCurrentCount() {
    return READ_COUNT.get();
  }
  private static final AtomicLong READ_COUNT = new AtomicLong(0);
  private static final AtomicLong IDS = new AtomicLong(0);
  private final long id = IDS.getAndIncrement();
  private final String inputUrl;
  private Table tableInfo;
  private String taskSql;
  private final BlockingQueue<List<String>> contentQueue;
  
  public ProducerThread(String inputUrl, BlockingQueue<List<String>> contentQueue) {
    this.inputUrl = inputUrl;
    this.contentQueue = contentQueue;
    //System.out.println("[READ THREAD]init #" +id + ", query:" + taskSql + ", " + this.getClass().getSimpleName());
  }
  
  public List<String> produce() throws SQLException, ClassNotFoundException {
    Produce task = getProduce();
    this.tableInfo = task.getTableInfo();
    this.taskSql = task.getTaskSql();
    try (DBClient sourceClient = new MysqlDBClient(inputUrl)) {
      sourceClient.setTable(tableInfo.getTableName());
      List<String> content = sourceClient.read(taskSql, tableInfo);
      return content;
    } finally {
      READ_COUNT.incrementAndGet();
    }
  }

//  @Overrdie
//  public Void call() throws Exception {

//  }

  @Override
  public String call() throws Exception {
    System.out.println("[READ THREAD] start #" + id + this.getClass().getSimpleName());
    contentQueue.put(produce());
    System.out.println("[READ THREAD] end #" + id + ", " +", query:" +taskSql+ this.getClass().getSimpleName());
    return "succeed";
  }
  
}
