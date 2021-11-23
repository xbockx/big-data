package com.linbo.data.mapreduce.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Description
 * @Author xbockx
 * @Date 11/23/2021
 */
public class ProvincePartition extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        final String phonePrefix = text.toString().substring(0, 3);
        int partition;
        if("136".equals(phonePrefix)) {
            partition = 0;
        } else if("137".equals(phonePrefix)) {
            partition = 1;
        }else if("138".equals(phonePrefix)) {
            partition = 2;
        }else if("139".equals(phonePrefix)) {
            partition = 3;
        } else {
            partition = 4;
        }
        return partition;
    }
}
