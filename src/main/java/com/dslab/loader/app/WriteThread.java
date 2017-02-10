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
  
import com.dslab.loader.DBClient.DBClient;
import java.sql.SQLException;
import java.util.*;  
import java.util.logging.Level;
import java.util.logging.Logger;
   
//import java.util.Random; 
public class WriteThread extends Thread { 
  private final Random random; 
  private final Queue buffer;
  private final DBClient dbClient;
  
  public WriteThread(String name, Queue buffer, DBClient dbClient, long seed) { 
    super(name); 
    this.buffer = buffer; 
    this.random = new Random(seed);
    this.dbClient = dbClient;
  } 
  public void run() { 
    try { 
      //while (true) {
        Block block = buffer.take(); 
        try {
          dbClient.write(block.getBlock());
        } catch (SQLException ex) {
          Logger.getLogger(WriteThread.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        Thread.sleep(random.nextInt(1000)); 
        System.out.println("write");
      //} 
    } catch (InterruptedException e) { } 
  }
 }          
 

