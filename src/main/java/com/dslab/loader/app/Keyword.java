/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.loader.app;

/**
 *
 * @author Shiou
 */
public class Keyword {
  private final String VALUE = "value";
  private final String START = "start";
  
  public int isKeyword(String colName) {
    if(VALUE.compareTo(colName) == 0 || START.compareTo(colName) == 0)
      return 0;
    else 
      return -1;
  }
}
