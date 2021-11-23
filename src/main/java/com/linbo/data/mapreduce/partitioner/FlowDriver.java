package com.linbo.data.mapreduce.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description
 * @Author xbockx
 * @Date 11/22/2021
 */
public class FlowDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setPartitionerClass(ProvincePartition.class);
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job, new Path("D:/temp/input/inputflow"));
        FileOutputFormat.setOutputPath(job, new Path("D:/temp/output/output1"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
