/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dsclab.loader.export;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Shiou
 */
public class DBClient implements Closeable{
  private static final Log LOG = LogFactory.getLog(DBClient.class);
  private Connection con = null;
  private Statement stmt = null;
  private DatabaseMetaData md;
  private ResultSet resultSet;
  
  public DBClient(final String url) throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
    try {
      con = DriverManager.getConnection(url);
      con.setAutoCommit(false);
      stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      stmt.setFetchSize(1000);
      stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
      md = con.getMetaData();
    } catch (RuntimeException ex) {
      //LOG.error("Could not connect",ex);
    }
  }
  
  public int getID(String table, String column, String key) throws SQLException {
    ResultSet rs = stmt.executeQuery("select id from "+table+" where "+column+" = '"+key+"'");
    while (rs.next()) {
      return rs.getInt("id");
    }
    return -1;
  }
  
  public int getID(String table, String column, int key) throws SQLException {
    ResultSet rs = stmt.executeQuery("select id from "+table+" where "+column+" = "+key);
    while (rs.next()) {
      return rs.getInt("id");
    }
    return -1;
  }
  
  public Map<Integer,Integer> getMapID(String table, String column, int key) throws SQLException {
    ResultSet rs = stmt.executeQuery("select * from "+table+" where "+column+" = "+key);
    Map<Integer,Integer> saveID = new HashMap<>();
    while (rs.next()) {
      saveID.put(rs.getInt("id"),rs.getInt("labelid"));
    }
    return saveID;
  }
  
  public List<Attribute_annotations> getAttributesAnnotations(int key) throws SQLException {
    ResultSet rs = stmt.executeQuery("select * from attribute_annotations where pathid = "+ key );
    List<Attribute_annotations> saveAttributeAnnotations = new ArrayList<>();
    while (rs.next()) {
      saveAttributeAnnotations.add(new Attribute_annotations(rs.getInt("attributeid"),rs.getInt("frame"),rs.getBoolean("value")));
    }
    return saveAttributeAnnotations;
  }
  
  public List<Box> getBox(String table, String column, int key) throws SQLException {
    ResultSet rs = stmt.executeQuery("select * from "+table+" where "+column+" = "+key);
    List<Box> saveBox = new ArrayList<>();
    while (rs.next()) {
      saveBox.add(new Box(rs.getInt("xtl"),rs.getInt("ytl"),rs.getInt("xbr"),rs.getInt("ybr"),
                          rs.getInt("frame"),rs.getBoolean("occluded"),rs.getBoolean("outside")));
    }
    return saveBox;
  }
  
  public String getLabelText(String table, String column, int key) throws SQLException {
    ResultSet rs = stmt.executeQuery("select * from "+table+" where "+column+" = "+key);
    String labelText = null;
    while (rs.next()) {
       labelText = rs.getString("text");
    }
    return labelText;
  }
  
  @Override
  public void close() {
    try {
      safeClose(resultSet);
      safeClose(stmt);
      safeClose(con);
    } catch (Exception ex) {
      LOG.error(ex);
    }
  }
  
  private static void safeClose(AutoCloseable close) throws Exception {
    if (close != null) {
      close.close();
    }
  }
}
