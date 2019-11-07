package com.study;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyEmployeeParitioner extends Partitioner<IntWritable, Employee> {

    @Override
    public int getPartition(IntWritable k2, Employee v2, int numPartition) {
        //如何建立分区
        if (v2.getSal() <1500){
            //放入1号分区中
            return 1%numPartition;
        }else if (v2.getSal() >=1500 && v2.getSal() <3000) {
            //放入2号分区中
            return 2%numPartition;
        }else {
            //放入0号分区中
            return 3%numPartition;
        }
    }
}
