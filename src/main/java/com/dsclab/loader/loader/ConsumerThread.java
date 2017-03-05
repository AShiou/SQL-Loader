/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dsclab.loader.loader;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author user
 */
public class ConsumerThread implements Callable<String> {
  private static final Log LOG = LogFactory.getLog(ConsumerThread.class);
  public static long getCurrentCount() {
    return WRITE_COUNT.get();
  }
  public static long getCurrentWrittingCount() {
    return WRITEING_COUNT.get();
  }
  public static long getIds() {
    return IDS.get();
  }
  private static final AtomicLong WRITEING_COUNT = new AtomicLong(0);
  private static final AtomicLong WRITE_COUNT = new AtomicLong(0);
  private static final AtomicLong IDS = new AtomicLong(0);
  private final long id = IDS.getAndIncrement();
  private final String outputUrl;
  private final BlockingQueue<List<String>> contentQueue;
  public ConsumerThread(String outputUrl, BlockingQueue<List<String>> contentQueue) {
    this.outputUrl = outputUrl;
    this.contentQueue = contentQueue;
    System.out.println("[WRITE THREAD]#" + id + ", " + this.getClass().getSimpleName());
  }
  
  public void consume(List<String> content) throws SQLException, ClassNotFoundException {
    //System.out.println("[CHIA7712] content.size:" + content.size());
    WRITEING_COUNT.incrementAndGet();
    try (DBClient targetClient = new PhoenixDBClient(outputUrl)) {
      targetClient.write(content);
    } finally {
      WRITE_COUNT.incrementAndGet();
    }
    
  }

  @Override
  public String call() throws Exception {
    System.out.println("[WRITE THREAD] start #" + id + ", " + this.getClass().getSimpleName());
    consume(contentQueue.take());
    System.out.println("[WRITE THREAD] end #" + id + ", " + this.getClass().getSimpleName());
    return "succeed";
  }
  
}
