/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dsclab.loader.export;

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
  
  public static final String DATABASE_URL = "phoenix.url";
  public static final String VIDEOS_SLUG = "videos.slug";
 
  public Configs(String propFileName) throws IOException {
      try {
      inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
      if (inputStream != null) {
	prop.load(inputStream);
      } else {
	throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
      }
    } catch (Exception e) {
      LOG.error(e);
    } finally {
      inputStream.close();
    }
  }

  public String getDBURL() {
    return prop.getProperty(DATABASE_URL);
  }

  public String getSlug() {
    return prop.getProperty(VIDEOS_SLUG);
  }

  public String getInputURL() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
}
