/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.loader.DBClient;

import com.dslab.loader.app.Keyword;
import com.dslab.loader.app.Schema;
import com.dslab.loader.app.Table;
import java.io.Closeable;
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
    private static final Log LOG
            = LogFactory.getLog(DBClient.class);
  //private static final Log LOG = LogFactory.getLog(SQLConnect.class);
  private String tableName;
  
  
  private static Connection con = null;
  private static Statement stmt = null;
  private static DatabaseMetaData md;
  private static ResultSet resultSet;
  
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
      LOG.info("Total column : "+rs.getInt(1));
      return rs.getInt(1);
    }
    return -1;
  }
  
  public List<Schema> getSchema() throws SQLException {
    List<Schema> tableSchema = new ArrayList<>();
    resultSet = con.getMetaData().getCatalogs();
    resultSet = md.getColumns(null, null, tableName, "%");
    while (resultSet.next()) {
      //System.out.println("Column Name of table " + table + " = "+ resultSet.getString(4) +" "+ resultSet.getString("DATA_TYPE"));
      Schema schema = new Schema(resultSet.getInt("DATA_TYPE"),resultSet.getString(4));

      /*if(tmp.getType().length() > 9) {     
        if(tmp.getType().substring(0,9).equals("unsupport")) {
        schema_tmp = new UnsupportedTable(table,tmp.getName(),tmp.getType());
        break;        
        }
      }*/      
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
        //System.out.println(rs.getString(3));
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
          System.out.println(colName);
          System.out.println("noooooooooooooooo");
        }
        else {
          GramPost.append(colName);
        }
        GramPost.append(" ");
        GramPost.append(colType);
        GramPost.append(",");
      }
    }
    //if(GramPost.length() >= 1)
    //  GramPost.setLength(GramPost.length()-1);
    GramPost.setCharAt(GramPost.length()-1,')');
    String Gram = GramPre.toString() + GramKey.toString() + GramPost.toString();
    //Gram = Gram.substring(0,Gram.length()-1);
    //Gram = Gram + ")";
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
    LOG.info("Success to create phoenix table.");
    //System.out.println("success");
  }
  
  public List<String> read (String readSQL, Table tableInfo) throws SQLException {
    ResultSet rs = stmt.executeQuery(readSQL);
    LOG.info(readSQL);
    List<String> contentList= new ArrayList<>();
    while (rs.next()) {
      Iterator<Schema> schemaList = tableInfo.getSchema().iterator();
      StringBuilder gram = new StringBuilder();
      gram.append("UPSERT INTO ");
      gram.append(tableName);
      gram.append(" VALUES(");
      while (schemaList.hasNext()) {
        Schema next = schemaList.next();
        //String colName = next.getColName();
        String value = rs.getString(next.getColName());
        if(next.getColType().compareTo("BOOLEAN") == 0) {
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
      //gram.append(";");
      String upsertSql = gram.toString();
      System.out.println(upsertSql);
      contentList.add(upsertSql);
    }
    return contentList;
  }
  
  public void write(List<String> writeSql) throws SQLException {
    Iterator<String> writeSqlList = writeSql.iterator();
    while (writeSqlList.hasNext()) {
      stmt.executeUpdate(writeSqlList.next());
      con.commit();
    }
    //stmt.executeUpdate("CREATE TABLE ttt(id INTEGER PRIMARY KEY,pathid INTEGER,xtl INTEGER,ytl INTEGER,xbr INTEGER,ybr INTEGER,frame INTEGER,occluded BOOLEAN,outside BOOLEAN)");
    //con.commit();
//rs = stmt.executeQuery("UPSERT INTO ttt VALUES(25,5,377,202,442,338,19,FALSE,FALSE)");
    
  }
  
  @Override
  public void close() {
    try {
      stmt.close();
      con.close();
      resultSet.close();
    } catch (SQLException ex) {
      Logger.getLogger(DBClient.class.getName()).log(Level.SEVERE, null, ex);
    }
  }  
}
