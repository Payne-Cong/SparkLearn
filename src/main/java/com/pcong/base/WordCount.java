package com.pcong.base;

import com.pcong.mapper.WordCountMapper;
import com.pcong.reduce.WordCountReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();


        Job job = Job.getInstance(conf, "word count");

        //设置jar加载路径
        job.setJarByClass(WordCount.class);

        //设置map和reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReduce.class);
        job.setReducerClass(WordCountReduce.class);

        //设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入和输出路径
        Path input = new Path("D:\\BaiduNetdiskDownload\\1.txt");
        Path output = new Path("D:\\BaiduNetdiskDownload\\temp\\");//必须要不存在

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        //提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);



    }
}
