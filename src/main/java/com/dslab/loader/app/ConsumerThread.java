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

/**
 *
 * @author user
 */
public class ConsumerThread implements Callable {
  private final String outputUrl;
  private final Buffer block;
  
  public ConsumerThread(String outputUrl,Buffer block) {
    this.outputUrl = outputUrl;
    this.block = block;
  }
  
  public void consume(List<String> content) throws SQLException, ClassNotFoundException {
    DBClient targetClient = new PhoenixDBClient(outputUrl);
    Iterator<String> contentList = content.iterator();
    while(contentList.hasNext()) {
      System.out.println("consume: "+contentList.next());
    }
    targetClient.write(content);
    targetClient.close();
  }

  @Override
  public Object call() throws Exception {
    List<String> content = block.take();
    consume(content);
    return Thread.currentThread().getName() + "结束";
  }
}
