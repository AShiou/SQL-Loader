/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.loader.app;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @author Shiou
 */
public class Buffer {
  private LinkedList list = new LinkedList();
  private int maxSize;
  private Lock lock = new ReentrantLock();
  private Condition condition;
  
  public Lock getLock() {
    return lock;
  }
  
  public void setLock(Lock lock) {
    this.lock = lock;
  }
  
  public Condition getCondition() {
    return condition;
  }
  
  public void setCondition(Condition condition) {
    this.condition = condition;
  }
  
  public LinkedList getList() {
    return list;
  }
  
  public void setList(LinkedList list) {
    this.list = list;
  }
  
  public int getMaxSize() {
    return maxSize;
  }
  
  public void setMaxSize(int maxSize) {
    this.maxSize = maxSize;
  }
  
  public Buffer(int maxSize) {
    super();
    this.maxSize = maxSize;
    this.condition = lock.newCondition();
  }
  
  public void put(List<String> content) {
    lock.lock();
    try {
      while (list.size() >= maxSize) {
        System.out.println(Thread.currentThread().getName()+" is full");
        this.condition.await();
      }
      list.addLast(content);
      System.out.println(Thread.currentThread().getName() + " add");
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
        System.out.println(Thread.currentThread().getName()+" null");
        this.condition.await();
      }
      list.removeFirst();
      System.out.println(Thread.currentThread().getName() + " consume");
      this.condition.signalAll();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {  
      lock.unlock();
    }
    return content;
  }
}