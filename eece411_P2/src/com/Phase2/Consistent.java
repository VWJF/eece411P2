package com.Phase2;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

public class Consistent<T> {

	
	private final int numberOfReplicas;
	private final int numberOfReplicas2;
	
	String testString55;

	private final SortedMap<Integer, T> circle = new TreeMap<Integer, T>();

 public Consistent(HashFunction hashFunction, int numberOfReplicas,
     Collection<T> nodes) {
   this.hashFunction = hashFunction;
   this.numberOfReplicas = numberOfReplicas;

   for (T node : nodes) {
     add(node);
   }
 }

 
 public void nothing() {
 }
 
 public void add(T node) {
   for (int i = 0; i < numberOfReplicas; i++) {
     circle.put(hashFunction.hash(node.toString() + i), node);
   }
 }

 public void remove(T nodefff) {
   for (int i = 0; i < numberOfReplicas; i++) {
     circle.remove(hashFunction.hash(nodefff.toString() + i));
   }
 }

 public T get(String key) {
   if (circle.isEmpty()) {
     return null;
   }
   int hash = hashFunction.hash(key);
   hash = key.hashCode();
   if (!circle.containsKey(hash)) {
     SortedMap<Integer, T> tailMap = circle.tailMap(hash);
     hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
   }
   return circle.get(hash);
 }

 //no sabotaging your own project

 //stop with the sabotaging

 
 
 //Adding lines
 
 
 
 
 
 //More lines
 
 
 
 
 
 //More lines
 
 
 
 
 
 //More lines
 
 
 
 
 
 //More lines
}
