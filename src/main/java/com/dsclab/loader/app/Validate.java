/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dsclab.loader.app;

import com.dsclab.loader.loader.Configs;
import com.dsclab.loader.validate.DBClient;
import com.dsclab.loader.validate.MysqlDBClient;
import com.dsclab.loader.validate.PhoenixDBClient;
import com.dsclab.loader.loader.Schema;
import com.dsclab.loader.validate.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.util.List;

/**
 *
 * @author dslab
 */
public class Validate {

  public static void main(final String[] args) throws Exception {
    String Module = args[1];
    switch (Module) {
      case "JSON":
        JsonNode obj;
        try (RandomAccessFile ra = new RandomAccessFile(args[2], "r")) {
          ObjectMapper mapper = new ObjectMapper();
          obj = mapper.readTree(ra.readLine());
        }
        JsonNode obj2;
        try (RandomAccessFile ra = new RandomAccessFile(args[3], "r")) {
          ObjectMapper mapper = new ObjectMapper();
          obj2 = mapper.readTree(ra.readLine());
        }
        if (obj.equals(obj2)) {
          System.out.println("Two JSON files are equal.");
        } else {
          System.out.println("Two JSON files are difference.");
        }
        break;
      case "TABLE":
        String configPath = args[2];
        Configs prop = new Configs(configPath);
        String tableName = args[3];

        DBClient sourceClient = new MysqlDBClient(prop.getInputURL());
        sourceClient.setTable(tableName);
        Table sourceTableInfo = new Table(tableName, sourceClient);

        DBClient targetClient = new PhoenixDBClient(prop.getOutputURL());
        targetClient.setTable(tableName.toUpperCase());
        Table targetTableInfo = new Table(tableName, targetClient);

        Boolean compareTable = compareTable(sourceTableInfo, targetTableInfo);
        Boolean compareTableContent = compareTableContent(sourceClient,sourceTableInfo,targetClient,targetTableInfo,tableName);
        if(compareTable==true && compareTableContent==true)
          System.out.println("Two tables are equal.");
        else 
          System.out.println("Two tables are difference.");
        break;
    }
  }
    
    

  public static Boolean compareTable(Table source, Table target) {
    if (source.getPriKey().toUpperCase().compareTo(target.getPriKey())!=0) {
      return false;
    }
    if (source.getTableName().compareTo(target.getTableName())!=0) {
      return false;
    }
    int i = 0;
    for (Schema sourceSchema : source.getSchema()) {
      Schema targetSchema = target.getSchema().get(i);
      if (sourceSchema.getColType() != targetSchema.getColType()) {
        return false;
      }
      if (sourceSchema.getColName().toUpperCase().compareTo(targetSchema.getColName())!=0) {
        return false;
      }
      i++;
    }
    return true;
  }
  
  public static Boolean compareTableContent(DBClient sourceClient, Table sourceTableInfo, DBClient targetClient, Table targetTableInfo, String tableName) throws SQLException{
    List<String> sourceContent = sourceClient.readMySql("select * from "+tableName, sourceTableInfo);
    List<String> targetContent = targetClient.readPhoenix("select * from "+tableName, targetTableInfo);
    int i=0;
    for (String sourceStr : sourceContent) {
      if(!sourceStr.equals(targetContent.get(i))) {
        return false;
      }
      i++;
    }
    return true;
  }

}
