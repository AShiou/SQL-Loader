/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.loader.app;

import java.util.List;

/**
 *
 * @author Shiou
 */
  public class Block {
  private final List<String> sqlList;

  public Block(List<String> sqlList) {
    this.sqlList = sqlList;
  } 
  
  public List<String> getBlock() {
    return sqlList;
  }
}
