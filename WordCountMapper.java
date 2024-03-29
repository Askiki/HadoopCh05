package com.study;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class WordCountMapper extends  Mapper<LongWritable,Text,Text,IntWritable>{
    protected void map(LongWritable key1,Text value1,Context context)
        throws IOException,InterruptedException{
        String data =value1.toString();
        String[] words=data.split("");
        for (String w:words){
            context.write(new Text(w),new IntWritable(1));
        }
    }
}

