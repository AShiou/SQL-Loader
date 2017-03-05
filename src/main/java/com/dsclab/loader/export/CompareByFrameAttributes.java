/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dsclab.loader.export;

import java.util.Comparator;

/**
 *
 * @author dslab
 */
public class CompareByFrameAttributes implements Comparator<Attribute_annotations> {
  public int compare(Attribute_annotations obj1, Attribute_annotations obj2){
    
        Attribute_annotations o1=(Attribute_annotations) obj1;
        Attribute_annotations o2=(Attribute_annotations) obj2;

        if(o1.getFrame()>o2.getFrame()){
            return 1;
        }

        if(o1.getFrame()<o2.getFrame()){
                return -1;
        }
        return 0;
    }
}