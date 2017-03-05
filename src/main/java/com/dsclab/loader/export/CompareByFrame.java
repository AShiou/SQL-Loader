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
public class CompareByFrame implements Comparator<Box> {
  public int compare(Box obj1, Box obj2){
    
        Box o1=(Box) obj1;
        Box o2=(Box) obj2;

        if(o1.getFrame()>o2.getFrame()){
            return 1;
        }

        if(o1.getFrame()<o2.getFrame()){
                return -1;
        }
        return 0;
    }
}
