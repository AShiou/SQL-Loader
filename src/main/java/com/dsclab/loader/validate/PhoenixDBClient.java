/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dsclab.loader.validate;

import com.dsclab.loader.loader.Schema;
import com.dsclab.loader.validate.DBClient;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Shiou
 */
public class PhoenixDBClient extends DBClient {

  public PhoenixDBClient(final String url) throws SQLException, ClassNotFoundException {
    super(url, "org.apache.phoenix.jdbc.PhoenixDriver");

  }

  
}
