package com.study;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DataMapper extends Mapper<LongWritable, Text, Text, Data> {
    @Override
    protected void map(LongWritable k1, Text v1, Context context)
        throws IOException, InterruptedException{
        String data = v1.toString();
        String[] words = data.split("\t");
        Data d = new Data();
        d.setTime(words[0]);
        d.setNumber(words[1]);
        d.setName(words[2]);
        String[] words3 = words[3].split(" ");
        d.setRank(Integer.parseInt(words3[0]));
        d.setOrder(Integer.parseInt(words3[1]));
        d.setWed(words[4]);
        context.write(new Text(d.getNumber()), d);
    }
}
