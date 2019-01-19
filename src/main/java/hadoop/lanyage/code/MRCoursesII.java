package hadoop.lanyage.code;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

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
public class MRCoursesII {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "partition");
        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(MRCoursesII.class);
        job.setMapperClass(AvgScoreAndNumPersonMapper.class);
        job.setReducerClass(AvgScoreAndNumPersonReducer.class);

        job.setOutputKeyClass(StudentBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setPartitionerClass(CoursePartitioner.class);
        job.setNumReduceTasks(4);

        Path inPath = new Path("output_courses");
        Path outPath = new Path("output3_courses");

        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        FileInputFormat.setInputPaths(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static class AvgScoreAndNumPersonMapper extends Mapper<Object, Text, StudentBean, NullWritable> {
        private final NullWritable value = NullWritable.get();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            double score = Double.parseDouble(splits[2]);
            DecimalFormat df = new DecimalFormat("#.0");
            String formattedScore = df.format(score);
            StudentBean studentBean = new StudentBean(splits[0], splits[1], formattedScore);
            context.write(studentBean, this.value);
        }
    }
    //根据Partition, 按照一定的规则将数组分配给不同的Reducer, 在分配之前就已经对数据按照一定的排序规则排好序了，然后Reducer将数据
    //写到不同的文件中
    public static class AvgScoreAndNumPersonReducer extends Reducer<StudentBean, NullWritable, StudentBean, NullWritable> {
        @Override
        protected void reduce(StudentBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println(key + " " + this.hashCode());
            for (NullWritable value : values) {
                context.write(key, value);
            }
        }
    }
}
