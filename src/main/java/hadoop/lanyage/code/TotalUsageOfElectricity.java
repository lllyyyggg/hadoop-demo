package hadoop.lanyage.code;


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

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * TotalUsage of Electricity, The Result is below
 * 1979 320
 * 1980 379
 * 1981 437
 * 1984 515
 * 1985 480
 * The data set is in the folder - classpath:resources.
 * core-site.xml is essential for the process.
 *
 * @author lanyage
 */
public class TotalUsageOfElectricity {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "total usage of task");

        job.setJarByClass(TotalUsageOfElectricity.class);
        job.setMapperClass(TotalUsageMapper.class);
        job.setReducerClass(TotalUsageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path inputPath = new Path("input");
        Path outputPath = new Path("output");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TotalUsageMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final Text key = new Text();
        private final IntWritable value = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString(), " ");

            if (stringTokenizer.hasMoreTokens()) {
                this.key.set(stringTokenizer.nextToken());
            } else {
                throw new RuntimeException("数据格式不正确");
            }
            while (stringTokenizer.hasMoreTokens()) {
                String token = stringTokenizer.nextToken();
                this.value.set(Integer.parseInt(token));
                context.write(this.key, this.value);
            }

        }
    }

    public static class TotalUsageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable value = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Integer sum = 0;
            Iterator<IntWritable> it = values.iterator();
            while (it.hasNext()) {
                sum += it.next().get();
            }
            this.value.set(sum);
            context.write(key, this.value);
        }
    }
}
