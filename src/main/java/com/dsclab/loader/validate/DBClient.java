/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dsclab.loader.validate;

import com.dsclab.loader.loader.Keyword;
import com.dsclab.loader.loader.Schema;
import com.dsclab.loader.validate.Table;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Shiou
 */
public class DBClient implements Closeable{
  private static final Log LOG = LogFactory.getLog(DBClient.class);
  private String tableName;
  private Connection con = null;
  private Statement stmt = null;
  private DatabaseMetaData md;
  private ResultSet resultSet;
  
  public DBClient(final String url, final String driverClass) throws ClassNotFoundException, SQLException {
    if (driverClass != null) {
      Class.forName(driverClass);
    }
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
  
  public void setTable(String tableName) {
    this.tableName = tableName;
  }
  
  public int getSqlCount() throws SQLException {
    ResultSet rs = stmt.executeQuery("select count(*) from "+tableName);
    while (rs.next()) {
      LOG.info("Table Name:"+tableName+" , Total column:"+rs.getInt(1));
      return rs.getInt(1);
    }
    return -1;
  }
  
  public List<Schema> getSchema() throws SQLException {
    List<Schema> tableSchema = new ArrayList<>();
    resultSet = con.getMetaData().getCatalogs();
    resultSet = md.getColumns(null, null, tableName, "%");
    while (resultSet.next()) {
      Schema schema = new Schema(resultSet.getInt("DATA_TYPE"),resultSet.getString(4));    
      tableSchema.add(schema);
     }
    return tableSchema;
  }
  
  public String getPriKey() throws SQLException{
    resultSet = md.getPrimaryKeys("", "", tableName);
    if(resultSet.next())
      return resultSet.getString("COLUMN_NAME");
    else {
      resultSet = con.getMetaData().getCatalogs();
      resultSet = md.getColumns(null, null, tableName, "%");
      while (resultSet.next()) {
        return resultSet.getString(4);
      }
      return null;
    }
  }
  
  public List<String> getTableList(String database) throws SQLException {
    List<String> tableList = new ArrayList<>();
      resultSet = md.getTables(database, null, "%", null);
      while (resultSet.next()) {
        tableList.add(resultSet.getString(3));
      }
    return tableList;
  }
  
  public void createTable(String tableName, Table tableInfo) throws SQLException {
    Keyword keyWord = new Keyword();
    
    Iterator<Schema> schemaList = tableInfo.getSchema().iterator();
    
    StringBuilder GramPre = new StringBuilder();
    GramPre.append("CREATE TABLE ");
    GramPre.append(tableName);
    GramPre.append("("); 
    
    StringBuilder GramKey = new StringBuilder();
    StringBuilder GramPost = new StringBuilder();
    
    while (schemaList.hasNext()) {
      Schema next = schemaList.next();
      String colName = next.getColName();
      String colType = next.getColType();
      if(colName.equals(tableInfo.getPriKey())) {
        GramKey.append(colName).append(" ").append(colType).append(" PRIMARY KEY,");
      }
      else {
        if(keyWord.isKeyword(colName) == 0 ) {
          GramPost.append("\"");
          GramPost.append(colName);
          GramPost.append("\"");
        }
        else {
          GramPost.append(colName);
        }
        GramPost.append(" ");
        GramPost.append(colType);
        GramPost.append(",");
      }
    }
    GramPost.setCharAt(GramPost.length()-1,')');
    String Gram = GramPre.toString() + GramKey.toString() + GramPost.toString();
    LOG.info(Gram);
    /*if(splitNum!=0) {
      Gram = Gram + "SALT_BUCKETS=" + splitNum;
    }*/
    //LOG.info(Gram);
    try {
      stmt.executeUpdate(Gram);
      con.commit();
    } catch (RuntimeException ex) {
      //LOG.error("Could not create table",ex);
    }
  }
  
  public List<String> readMySql (String readSQL, Table tableInfo) throws SQLException {
    ResultSet rs = stmt.executeQuery(readSQL);
    List<String> contentList= new ArrayList<>();
    while (rs.next()) {
      Iterator<Schema> schemaList = tableInfo.getSchema().iterator();
      StringBuilder gram = new StringBuilder();
      gram.append(" (");
      while (schemaList.hasNext()) {
        Schema next = schemaList.next();
        String value = rs.getString(next.getColName());
        if(next.getColType().compareTo("VARCHAR") == 0) {
          gram.append("'");
          gram.append(value);
          gram.append("'");
        }else if(next.getColType().compareTo("BOOLEAN") == 0) {
          if(value.compareTo("0") == 0) {
            gram.append("FALSE");
          }
          else {
            gram.append("TRUE");
          }
        }
        else {
          gram.append(value);
        }
        gram.append(",");
      }
      gram.setCharAt(gram.length()-1,')');
      //System.out.println(gram.toString());
      contentList.add(gram.toString());
    }
    return contentList;
  }
  
    public List<String> readPhoenix (String readSQL, Table tableInfo) throws SQLException {
    ResultSet rs = stmt.executeQuery(readSQL);
    List<String> contentList= new ArrayList<>();
    while (rs.next()) {
      Iterator<Schema> schemaList = tableInfo.getSchema().iterator();
      StringBuilder gram = new StringBuilder();
      gram.append(" (");
      while (schemaList.hasNext()) {
        Schema next = schemaList.next();
        String value = rs.getString(next.getColName());
        if(next.getColType().compareTo("VARCHAR") == 0) {
          gram.append("'");
          gram.append(value);
          gram.append("'");
        }else if(next.getColType().compareTo("BOOLEAN") == 0) {
          if(value.compareTo("false") == 0) {
            gram.append("FALSE");
          }
          else {
            gram.append("TRUE");
          }
        }
        else {
          gram.append(value);
        }
        gram.append(",");
      }
      gram.setCharAt(gram.length()-1,')');
      //System.out.println(gram.toString());
      contentList.add(gram.toString());
    }
    return contentList;
  }
  
  public void write(List<String> writeSql) throws SQLException {
    Iterator<String> writeSqlList = writeSql.iterator();
    while (writeSqlList.hasNext()) {
      stmt.executeUpdate(writeSqlList.next());
      con.commit();
    }
  //stmt.executeQuery("UPSERT INTO ttt VALUES(25,5,377,202,442,338,19,FALSE,FALSE)");
  //con.commit();
    
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
  
  public Statement getStatement() {
    return stmt;
  }
  
  public Connection getConnection() {
    return con;
  }
  
}

