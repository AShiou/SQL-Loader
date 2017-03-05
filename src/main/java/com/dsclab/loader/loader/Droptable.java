/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dsclab.loader.loader;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

/**
 *
 * @author dslab
 */
public class Droptable {
  public Droptable() throws SQLException, ClassNotFoundException {
    DBClient sourceClient = new PhoenixDBClient("jdbc:phoenix:phoenix-01");
    sourceClient.droptable();
    sourceClient.close();
  }
}
