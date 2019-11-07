package com.study;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DataReducer extends Reducer<Text, Data, Text, Data> {
    @Override
    protected void reduce(Text k3, Iterable<Data> v3, Context context)
        throws IOException, InterruptedException{
        for (Data d:v3){
            if (d.getRank()==2 &&d.getOrder()==1){
                context.write(new Text(" "),d);
            }
        }
    }
}
