package com.study;

import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SaleMapper extends Mapper<LongWritable, Text, Text, Sale> {
    @Override
    protected void map(LongWritable k1, Text v1, Context context)
        throws IOException, InterruptedException{
        String data = v1.toString();
        String[] words = data.split(",");
        String[] year = words[2].split("-");
        Sale sale = new Sale();
        sale.setProd_id(Integer.parseInt(words[0]));
        sale.setCust_id(Integer.parseInt(words[1]));
        sale.setTime(year[0]);
        sale.setChannel_id(Integer.parseInt(words[3]));
        sale.setPromo_id(Integer.parseInt(words[4]));
        sale.setQuantity_sold(Integer.parseInt(words[5]));
        sale.setAmount_sold(Float.parseFloat(words[6]));
        context.write(new Text(sale.getTime()), sale);
    }
}
