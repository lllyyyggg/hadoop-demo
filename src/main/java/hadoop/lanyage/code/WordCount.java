package hadoop.lanyage.code;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Word Count Task
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 获取配置
        Configuration conf = new Configuration();
        // 创建任务
        Job job = Job.getInstance(conf, "word count");
        // 指定jar包的class路径
        job.setJarByClass(WordCount.class);
        // 指定mapper.class
        job.setMapperClass(WordCountMapper.class);
        // 指定combiner.class
        job.setCombinerClass(WordCountReducer.class);
        // 指定reducer.class
        job.setReducerClass(WordCountReducer.class);
        // 指定输出的key的类型
        job.setOutputKeyClass(Text.class);
        // 指定输出的value的类型
        job.setOutputValueClass(IntWritable.class);

        Path inputPath = new Path("input");
        Path outputPath = new Path("output");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        // 指定input的路径
        FileInputFormat.setInputPaths(job, inputPath);
        // 指定output的路径
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final Text key = new Text();
        private final IntWritable value = new IntWritable(1);
        public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String word : words) {
                this.key.set(word);
                context.write(this.key, this.value);
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable value = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> it = values.iterator();
            while (it.hasNext()) {
                IntWritable value = it.next();
                sum += value.get();
            }
            this.value.set(sum);
            context.write(key, this.value);
        }
    }
}

