/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.loader.DBClient;

import java.sql.SQLException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author dslab
 */
public class MysqlDBClient extends DBClient {
  private static final Log LOG
            = LogFactory.getLog(MysqlDBClient.class);
  public MysqlDBClient(final String url) throws SQLException, ClassNotFoundException {
    super(url, "com.mysql.jdbc.Driver");
  }
  
}
