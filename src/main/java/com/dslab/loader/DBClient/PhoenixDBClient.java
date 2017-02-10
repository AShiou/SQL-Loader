/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.loader.DBClient;

import java.sql.SQLException;

/**
 *
 * @author Shiou
 */
public class PhoenixDBClient extends DBClient {
  
  public PhoenixDBClient(final String url) throws SQLException, ClassNotFoundException {
    super(url, "org.apache.phoenix.jdbc.PhoenixDriver");
  }
  
}
