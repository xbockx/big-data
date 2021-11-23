package com.linbo.data.mapreduce.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description
 * @Author xbockx
 * @Date 11/22/2021
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String line = value.toString();
        final String[] split = line.split("\t");
        String phone = split[1];
        String up = split[split.length - 3];
        String down = split[split.length - 2];

        outK.set(phone);
        outV.setUpFlow(Long.parseLong(up));
        outV.setDownFlow(Long.parseLong(down));
        outV.setSumFlow();

        context.write(outK, outV);
    }
}
