/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.me.neu.popular_tag_year;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 * @author Manali
 */
public class Runner {
    public static void main(String[] args) throws Exception
  {
        JobConf conf = new JobConf(Runner.class);
        conf.setJobName("tag-year");
        
        conf.setMapperClass(Mapper1.class);
        
       // conf.setOutputKeyComparatorClass(DescendingIntComparable.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        
        conf.setReducerClass(Reducer1.class);
        
        
        // take the input and output from the command line
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);

  }
   
}
