package hadoop.lanyage.code;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * 1. 统计每门课的参考人数和课程平均分
 * 2. 统计每门课参考学生的平均分，并且按照课程存入不同的结果文件，并且按平均分从高到低排序，分数保留一位小数
 * 3. 求出每门课程参考学生成绩最高的学生的信息：课程，姓名和平均分
 * <p>
 * Input:(课程名，学生名，多次考试的分数)
 * computer,huangxiaoming,85,86,41,75,93,42,85
 * computer,xuzheng,54,52,86,91,42
 * computer,huangbo,85,42,96,38
 * english,zhaobenshan,54,52,86,91,42,85,75
 * english,liuyifei,85,41,75,21,85,96,14
 * algorithm,liuyifei,75,85,62,48,54,96,15
 * computer,huangjiaju,85,75,86,85,85
 * english,liuyifei,76,95,86,74,68,74,48
 * english,huangdatou,48,58,67,86,15,33,85
 * algorithm,huanglei,76,95,86,74,68,74,48
 * algorithm,huangjiaju,85,75,86,85,85,74,86
 * computer,huangdatou,48,58,67,86,15,33,85
 * english,zhouqi,85,86,41,75,93,42,85,75,55,47,22
 * english,huangbo,85,42,96,38,55,47,22
 * algorithm,liutao,85,75,85,99,66
 * computer,huangzitao,85,86,41,75,93,42,85
 * math,wangbaoqiang,85,86,41,75,93,42,85
 * computer,liujialing,85,41,75,21,85,96,14,74,86
 * computer,liuyifei,75,85,62,48,54,96,15
 * computer,liutao,85,75,85,99,66,88,75,91
 * computer,huanglei,76,95,86,74,68,74,48
 * english,liujialing,75,85,62,48,54,96,15
 * math,huanglei,76,95,86,74,68,74,48
 * math,huangjiaju,85,75,86,85,85,74,86
 * math,liutao,48,58,67,86,15,33,85
 * english,huanglei,85,75,85,99,66,88,75,91
 * math,xuzheng,54,52,86,91,42,85,75
 * math,huangxiaoming,85,75,85,99,66,88,75,91
 * math,liujialing,85,86,41,75,93,42,85,75
 * english,huangxiaoming,85,86,41,75,93,42,85
 * algorithm,huangdatou,48,58,67,86,15,33,85
 * algorithm,huangzitao,85,86,41,75,93,42,85,75
 */
public class MRCourses2 {

    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration conf1 = new Configuration();
        Configuration conf2 = new Configuration();

        FileSystem fs1 = FileSystem.get(conf1);
        FileSystem fs2 = FileSystem.get(conf2);

        Job job1 = Job.getInstance(conf1);
        Job job2 = Job.getInstance(conf2);

        job1.setJarByClass(MRCourses2.class);
        job1.setMapperClass(AvgScoreMapper.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        Path pathIn1 = new Path("input_courses");
        Path pathOut1 = new Path("output_courses");
        if (fs1.exists(pathOut1)) {
            fs1.delete(pathOut1, true);
        }

        FileInputFormat.setInputPaths(job1, pathIn1);
        FileOutputFormat.setOutputPath(job1, pathOut1);

        job2.setMapperClass(AvgScoreAndNumPersonMapper.class);
        job2.setReducerClass(AvgScoreAndNumPersonReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);

        Path pathIn2 = new Path("output_courses");
        Path pathOut2 = new Path("output2_courses");
        if (fs2.exists(pathOut2)) {
            fs2.delete(pathOut2, true);
        }

        FileInputFormat.setInputPaths(job2, pathIn2);
        FileOutputFormat.setOutputPath(job2, pathOut2);

        JobControl control = new JobControl("AvgScore");

        ControlledJob cjob1 = new ControlledJob(job1.getConfiguration());
        ControlledJob cjob2 = new ControlledJob(job2.getConfiguration());
        cjob2.addDependingJob(cjob1);

        control.addJob(cjob1);
        control.addJob(cjob2);

        Thread thread = new Thread(control);
        thread.start();

        while (!control.allFinished()) {
            Thread.sleep(1000);
        }

        System.exit(0);
    }

    public static class AvgScoreMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private final Text key = new Text();
        private final DoubleWritable value = new DoubleWritable();

        /**
         * 进入环形缓冲区之后会进行排序
         */
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] content = value.toString().split(",");
            StringBuilder outKey = new StringBuilder();
            outKey.append(content[0]).append("\t").append(content[1]);
            int sum = 0;
            int i = 2;
            while (i < content.length) {
                sum += Integer.parseInt(content[i]);
                i++;
            }
            double avg = 1D * sum / (i - 2);
            this.key.set(outKey.toString());
            this.value.set(avg);
            context.write(this.key, this.value);
        }
    }

    public static class AvgScoreAndNumPersonMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private final Text key = new Text();
        private final DoubleWritable value = new DoubleWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] content = value.toString().split("\t");
            this.key.set(content[0]);
            this.value.set(Double.parseDouble(content[2]));
            context.write(this.key, this.value);
        }
    }

    public static class AvgScoreAndNumPersonReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        private final Text value = new Text();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            Iterator<DoubleWritable> it = values.iterator();
            while (it.hasNext()) {
                DoubleWritable curr = it.next();
                sum += curr.get();
                count++;
            }
            String outValue = (1D * sum / count) + " " + count;
            this.value.set(outValue);
            context.write(key, this.value);
        }
    }
}
