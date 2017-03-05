/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dsclab.loader.export;

import java.util.List;

/**
 *
 * @author dslab
 */
public class Box {
  private final int xtl;
  private final int ytl;
  private final int xbr;
  private final int ybr;
  private final int frame;
  private final int outside;
  private final int occluded;
  //private final List<Attributes> attributes;
  
  public Box(int xtl, int ytl, int xbr, int ybr, int frame, boolean outside, boolean occluded) {
    this.xtl = xtl;
    this.ytl = ytl;
    this.xbr = xbr;
    this.ybr = ybr;
    this.frame = frame;
    if(outside==true) {
      this.outside = 1;
    } else { 
      this.outside = 0;
    }
    if(occluded==true) {
      this.occluded = 1;
    } else {
      this.occluded = 0;
    }
    //this.attributes = attributes;
  }
  
  public int getXtl() {
    return xtl;
  }
  
  public int getYtl() {
    return ytl;
  }
  
  public int getXbr() {
    return xbr;
  }
  
  public int getYbr() {
    return ybr;
  }
  
  public int getFrame() {
    return frame;
  }
  
  public int getOutside() {
    return outside;
  }
  
  public int getOccluded() {
    return occluded;
  }
  
  public boolean getOccludedBoolean() {
    if(occluded==1)
      return true;
    else
      return false;
  }
  
  /*public List<Attributes> getAttributes() {
    return attributes;
  }*/
}
