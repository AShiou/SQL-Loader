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
public class Attribute_annotations {
  private final int attributesId;
  private final int frame;
  private final Boolean value;
  
  public Attribute_annotations(int attributesId, int frame, Boolean value) {
    this.attributesId = attributesId;
    this.frame = frame;
    this.value = value;
  }
  
  public int getAttributesId() {
    return attributesId;
  }
  
  public int getFrame() {
    return frame;
  }
  
  public Boolean getValue() {
    return value;
  }
}
