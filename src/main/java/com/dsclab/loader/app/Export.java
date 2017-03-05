/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dsclab.loader.app;

import com.dsclab.loader.export.Attribute_annotations;
import com.dsclab.loader.export.Box;
import com.dsclab.loader.export.CompareByFrame;
import com.dsclab.loader.export.CompareByFrameAttributes;
import com.dsclab.loader.export.Configs;
import com.dsclab.loader.export.DBClient;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.json.simple.JSONObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 * @author dslab
 */
public class Export {

  public static void main(final String[] args) throws Exception {

    String configPath = args[1];
    Configs prop = new Configs(configPath);
    DBClient connect = new DBClient(prop.getDBURL());
    int videosID = connect.getID("videos", "slug", prop.getSlug());
    int segmentsID = connect.getID("segments", "videoid", videosID);
    int jobsID = connect.getID("jobs", "segmentid", segmentsID);

    Map<Integer, Integer> pathsID = connect.getMapID("paths", "jobid", jobsID);

    JSONObject obj = new JSONObject();
    //Gson gson = new Gson();   
    int i = 0;
    for (Map.Entry<Integer, Integer> entry : pathsID.entrySet()) {
      JSONObject pathObj = new JSONObject();
      List<Box> boxes = connect.getBox("boxes", "pathid", entry.getKey());
      List<Attribute_annotations> attributeAnnotations = connect.getAttributesAnnotations(entry.getKey());
      List<Box> tmp = linearList(boxes,attributeAnnotations);
      JSONObject boxObject = new JSONObject();
      for (Box targetBox : tmp) {
        //System.out.println(targetBox.getFrame());
        JSONObject boxContent = new JSONObject();
        boxContent.put("xtl",targetBox.getXtl());
        boxContent.put("ytl",targetBox.getYtl());
        boxContent.put("xbr",targetBox.getXbr());
        boxContent.put("ybr",targetBox.getYbr());
        boxContent.put("occluded",targetBox.getOccluded());
        boxContent.put("outside", targetBox.getOutside());
        boxObject.put(targetBox.getFrame(),boxContent);
      }
      pathObj.put("boxes", boxObject);
      pathObj.put("label", connect.getLabelText("labels", "id", entry.getValue()));
      obj.put(i, pathObj);
      i++;
    }
    Path path = Paths.get(prop.getSlug()+".json");
    Files.createFile(path);
    try(RandomAccessFile ra = new RandomAccessFile(prop.getSlug()+".json", "rw")) {
      ra.writeBytes(obj.toJSONString());
    }   
    //System.out.println(obj);
  }

  public static List<Box> linearList(List<Box> boxes,List<Attribute_annotations> attributeAnnotations) {
    List<Box> interval = new ArrayList<>();
    Collections.sort(boxes, new CompareByFrame());
    Box preBox = null;
    for (Box targetBox : boxes) {
      if (preBox == null){
        interval.add(targetBox);
      } else {
        float fdiff = (float) (targetBox.getFrame() - preBox.getFrame());
        float xtlr = (targetBox.getXtl() - preBox.getXtl()) / fdiff;
        float ytlr = (targetBox.getYtl() - preBox.getYtl()) / fdiff;
        float xbrr = (targetBox.getXbr() - preBox.getXbr()) / fdiff;
        float ybrr = (targetBox.getYbr() - preBox.getYbr()) / fdiff;
        for (int i = preBox.getFrame() + 1; i <= targetBox.getFrame(); i++) {
          int off = i - preBox.getFrame();
          int xtl = (int) (preBox.getXtl() + xtlr * off);
          int ytl = (int) (preBox.getYtl() + ytlr * off);
          int xbr = (int) (preBox.getXbr() + xbrr * off);
          int ybr = (int) (preBox.getYbr() + ybrr * off);
          if(i == targetBox.getFrame()) {
            interval.add(targetBox);
          } else { 
              interval.add(new Box(xtl, ytl, xbr, ybr, i, false, preBox.getOccludedBoolean()));
          }
        }
      }
      preBox = targetBox;
    }
    return interval;
  }

}
