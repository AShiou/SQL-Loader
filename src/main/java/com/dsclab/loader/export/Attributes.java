/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dsclab.loader.export;

/**
 *
 * @author dslab
 */
public class Attributes {
  private final int attributesId;
  private final String attributesText;
  
  public Attributes(int attributesId, String attributesText) {
    this.attributesId = attributesId;
    this.attributesText = attributesText;
  }
  
  public int getAttributesId() {
    return attributesId;
  }
  
  public String getAttributesText() {
    return attributesText;
  }
}
