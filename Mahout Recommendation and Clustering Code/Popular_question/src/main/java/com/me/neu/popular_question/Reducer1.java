/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.me.neu.Popular_question;

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
    public void reduce(Text tagName, Iterator<Text> favCount, OutputCollector<Text,Text> output, Reporter rprtr) throws IOException {
        TreeMap<Integer, Integer> tmap = new TreeMap<Integer, Integer>();
        
        while (favCount.hasNext())
        {
          String post_fav[] = favCount.next().toString().split(",");
          tmap.put(Integer.parseInt(post_fav[1]),Integer.parseInt(post_fav[0]));
        }
        
        output.collect(new Text(tagName+","+tmap.lastEntry().getKey()+","+tmap.pollLastEntry().getValue()), new Text());

    }
    
}
