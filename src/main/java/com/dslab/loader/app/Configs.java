/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.loader.app;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Shiou
 */
public class Configs {
  
  private static final Log LOG = LogFactory.getLog(Configs.class);
  
  String result = "";
  InputStream inputStream;
  Properties prop = new Properties();
  
  public static final String SOURCE_DATABASE_DBNAME = "source.mysql.dbname";
  public static final String SOURCE_DATABASE_URL = "source.mysql.url";
  public static final String TARGET_DATABASE_URL ="target.phoenix.url";
  public static final String TASK_SIZE = "task.size";
  public static final String READ_THREAD = "read.thread";
  public static final String WRITE_THREAD = "write.thread";
  public static final String BLOCK_NUMBER = "block.number";
 
  public Configs(String propFileName) throws IOException {
      try {
      inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
      if (inputStream != null) {
	prop.load(inputStream);
      } else {
	throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
      }
    } catch (Exception e) {
      System.out.println("Exception: " + e);
    } finally {
      inputStream.close();
    }
  }
  
  public String getInputdb() {
    return prop.getProperty(SOURCE_DATABASE_DBNAME);
  }
  
  public String getInputURL() {
    return prop.getProperty(SOURCE_DATABASE_URL);
  }
  
  public String getOutputURL() {
    return prop.getProperty(TARGET_DATABASE_URL);
  }
  
  public int getTaskSize() {
    return Integer.parseInt(prop.getProperty(TASK_SIZE));
  }
  
  public int getReadThread() {
    return Integer.parseInt(prop.getProperty(READ_THREAD));
  }
  
  public int getWriteThread() {
    return Integer.parseInt(prop.getProperty(WRITE_THREAD));
  }
  
  public int getBlockNumber() {
    return Integer.parseInt(prop.getProperty(BLOCK_NUMBER));
  }
  
}
