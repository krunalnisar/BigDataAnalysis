/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.me.neu.stackoverflow;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


/**
 *
 * @author Manali
 */
public class Reducer1 extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

    public static int count = 1;
    @Override
    public void reduce(Text userId, Iterator<Text> tagName, OutputCollector<Text,Text> output, Reporter rprtr) throws IOException {
        HashMap<String,Integer> hm = new HashMap();
        
        while (tagName.hasNext())
        {
          String tags = tagName.next().toString();
          if(hm.containsKey(tags)){
              int latest = hm.get(tags);
              hm.put(tags,++latest);
          }else{
              hm.put(tags,1);
          }
          
        }
        
        Set set = hm.entrySet();
        Iterator iterator = set.iterator();
        while(iterator.hasNext()) {
         Map.Entry mentry = (Map.Entry)iterator.next();
         //System.out.print("key is: "+ mentry.getKey() + " & Value is: ");
         //System.out.println(mentry.getValue());
         count = count+1;
         String s = mentry.getKey() +","+mentry.getValue();
         output.collect(new Text(count+","+userId+","+s), new Text());
      }
        
    }
    
}
