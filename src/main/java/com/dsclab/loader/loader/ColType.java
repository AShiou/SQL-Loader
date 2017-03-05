/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dsclab.loader.loader;

import java.sql.Types;

/**
 *
 * @author Shiou
 */
public class ColType {
  
  String colType;
  
  public ColType(int type) {
    this.colType = TransType(type);
  }
  
  public String TransType(int type) {
    switch(type){
      case Types.ARRAY:
        return "unsupport type <ARRAY>,please create by hand operation.";
        //throw new RuntimeException("Should create by hand operation.");
      case Types.BIGINT:
        return PhoenixType("Long");
      case Types.BINARY:
      case Types.LONGVARBINARY:
      case Types.VARBINARY:
        return PhoenixType("byte[]");
      case Types.BIT:
      case Types.BOOLEAN:
        return PhoenixType("Boolean");
      case Types.BLOB:
        //throw new RuntimeException("Unsupported jdbc type for phoenix : " + type);
        return "unsupport jdbc type <BLOB> for phoenix";
      case Types.CHAR:
      case Types.LONGNVARCHAR:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.VARCHAR:
        return PhoenixType("String");
      case Types.CLOB:
        return "unsupport jdbc type <CLOB> for phoenix";
        //throw new RuntimeException("Unsupported jdbc type for phoenix : " + type);
      case Types.DATALINK:
        return "unsupport jdbc type <DATALINK> for phoenix";
        //throw new RuntimeException("Unsupported jdbc type for phoenix : " + type);
      case Types.DATE:
        return "DATE";
      case Types.DECIMAL:    
      case Types.NUMERIC:
        return "DECIMAL";
      case Types.DISTINCT:
      case Types.JAVA_OBJECT:
      case Types.OTHER:
      case Types.REF:
      case Types.REF_CURSOR:
      case Types.STRUCT:
        return "unsupport jdbc type <STRUCT> for phoenix";
        //throw new RuntimeException("Unsupported jdbc type for phoenix : " + type);
      case Types.DOUBLE:  
        return PhoenixType("Double");
      case Types.FLOAT:
      case Types.REAL:
        return PhoenixType("Float");
      case Types.INTEGER:
        return PhoenixType("Integer");
      case Types.NCLOB:
       return "unsupport jdbc type <NCLOB> for phoenix";
        //throw new RuntimeException("Unsupported jdbc type for phoenix : " + type);
      case Types.NULL:
        return "unsupport jdbc type <NULL> for phoenix";
        //throw new RuntimeException("Unsupported jdbc type for phoenix : " + type);
      case Types.ROWID:
        return "unsupport jdbc type <ROWID> for phoenix";
        //throw new RuntimeException("Unsupported jdbc type for phoenix : " + type);
      case Types.SMALLINT:
        return PhoenixType("Short");
      case Types.SQLXML:
        return "unsupport jdbc type <SQLXML> for phoenix";
        //throw new RuntimeException("Unsupported jdbc type for phoenix : " + type);
      case Types.TIME:
      case Types.TIME_WITH_TIMEZONE:
        return "TIME";
      case Types.TIMESTAMP:
      case Types.TIMESTAMP_WITH_TIMEZONE:
        return "TIMESTAMP";
      case Types.TINYINT:
        return PhoenixType("Byte");
      default:
        return "unsupport jdbc type "+type+" for phoenix";
        //throw new RuntimeException("Unsupported jdbc type for phoenix : " + type);
    }    
  }
    
  public String PhoenixType(String type) {
    switch(type) {
      case "Integer":
        return "INTEGER";
      case "Long":
        return "BIGINT";
      case "Byte":
        return "TINYINT";
      case "Short":
        return "SMALLINT";
      case "Float":
        return "FLOAT";
      case "Double":
        return "DOUBLE";
      case "Boolean":
        return "BOOLEAN";
      case "String":         
        return "VARCHAR";
      case "byte[]":
        return "VARBINARY";
      default:
        return "unsupport java type "+type+" for phoenix";
        //throw new RuntimeException("Unsupported java type for phoenix : " + type);  
    }
  }
  
  public String getType() {
    return colType;
  }
  
}

