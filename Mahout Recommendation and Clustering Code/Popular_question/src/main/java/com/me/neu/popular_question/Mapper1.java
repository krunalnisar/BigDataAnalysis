/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.me.neu.Popular_question;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author Manali
 */
public class Mapper1 extends MapReduceBase 
                        implements Mapper<LongWritable, Text, Text,Text>{
    
    @Override
    public void map(LongWritable k1, Text v1, OutputCollector<Text,Text> output, Reporter rprtr) throws IOException {
        String []line=v1.toString().split(",");
        String []tagNames = line[1].split(">");
        String favCount = line[2];
        for(String tagName:tagNames){
            output.collect(new Text(tagName), new Text(line[0]+","+favCount));
        }
        
    }
    
}
