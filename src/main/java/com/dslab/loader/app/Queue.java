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
public class Queue {
  private final Block[] buffer;
  private int tail;
  private int head;
  private int count;
  
  public Queue(int blockMax) { 
    this.buffer = new Block[blockMax];
    this.tail = 0;
    this.head = 0;
    this.count = 0;
  } 
  
  public synchronized void put(Block block) throws InterruptedException { 
    //System.out.println(Thread.currentThread().getName() + " puts " + block);
    while (count >= buffer.length) { 
      wait();
    } 
    buffer[tail] = block; 
    tail = (tail + 1) % buffer.length; 
    count++; 
    notifyAll(); 
  } 
  
// 取得block 
  public synchronized Block take() throws InterruptedException { 
    while (count <= 0) {
      wait(); 
    } 
    Block block = buffer[head]; 
    head = (head + 1) % buffer.length; count--;
    notifyAll(); 
    //System.out.println(Thread.currentThread().getName() + " takes " + block);
    return block; 
  }
}
