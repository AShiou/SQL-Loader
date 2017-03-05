/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dsclab.loader.loader;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @author Shiou
 */
public class Buffer {
  private final LinkedList list = new LinkedList();
  private final int maxSize;
  private final Lock lock = new ReentrantLock();
  private final Condition condition;
  
  public Buffer(int maxSize) {
    super();
    this.maxSize = maxSize;
    this.condition = lock.newCondition();
  }
  
  public void put(List<String> content) {
    lock.lock();
    try {
      while (list.size() >= maxSize) {
        this.condition.await();
      }
      list.addLast(content);
      this.condition.signalAll();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      lock.unlock();
    }   
  }
  
  public List<String> take() {
    lock.lock();
    List<String> content= null;
    try {
      while (list.size() <= 0) {
        this.condition.await();
      }
      content = (List<String>) list.removeFirst();
      this.condition.signalAll();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {  
      lock.unlock();
    }
    return content;
  }
  
}