package com.pcong.base;

import com.pcong.bean.FlowBean;
import com.pcong.mapper.FlowCountMapper;
import com.pcong.reduce.FlowCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowsumDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 6 指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowsumDriver.class);

        // 2 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        // 3 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 4 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        Path input = new Path("phone_data.txt");
        Path output = new Path("D:\\BaiduNetdiskDownload\\temp\\");

        // 5 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);


        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);


    }
}
