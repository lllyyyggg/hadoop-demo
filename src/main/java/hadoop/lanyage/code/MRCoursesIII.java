package hadoop.lanyage.code;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class MRCoursesIII {

    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration conf1 = new Configuration();
        Configuration conf2 = new Configuration();
        Job job1 = Job.getInstance(conf1, "job1");
        Job job2 = Job.getInstance(conf2, "job2");
        FileSystem fs1 = FileSystem.get(conf1);
        FileSystem fs2 = FileSystem.get(conf2);

        job1.setJarByClass(MRCoursesIII.class);
        job1.setMapperClass(TopOneMapper.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(StudentBean.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(StudentBean.class);


        // 此处会根据CoursePartionerII将内容分发给下一轮的4个Mapper
        job1.setPartitionerClass(CoursePartitionerII.class);
        job1.setNumReduceTasks(4);

        Path pathIn = new Path("input_courses");
        Path pathOut = new Path("output4_courses");
        if (fs1.exists(pathOut)) {
            fs1.delete(pathOut, true);
        }
        FileInputFormat.setInputPaths(job1, pathIn);
        FileOutputFormat.setOutputPath(job1, pathOut);

        job2.setMapperClass(TopOneMapper2.class);
        job2.setReducerClass(TopOneReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(StudentBean.class);
        job2.setOutputValueClass(NullWritable.class);

        Path pathIn2 = new Path("output4_courses");
        Path pathOut2 = new Path("output5_courses");
        if (fs2.exists(pathOut2)) {
            fs2.delete(pathOut2, true);
        }
        FileInputFormat.setInputPaths(job2, pathIn2);
        FileOutputFormat.setOutputPath(job2, pathOut2);


        JobControl control = new JobControl("TopOneScore");

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

    public static class TopOneMapper extends Mapper<Object, Text, IntWritable, StudentBean> {
        private final IntWritable key = new IntWritable();
        private final StudentBean value = new StudentBean();
        private final List<String> scoreList = new ArrayList<>();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //System.out.println(this.hashCode());
            String[] splits = value.toString().split(",");
            long sum = 0;
            for (int i = 2; i < splits.length; i++) {
                scoreList.add(splits[i]);
                sum += Long.parseLong(splits[i]);
            }
            Collections.sort(scoreList);
            this.key.set(Integer.parseInt(scoreList.get(scoreList.size() - 1)));
            double avg = 1D * sum / (splits.length - 2);
            this.value.setCourse(splits[0]);
            this.value.setName(splits[1]);
            this.value.setScore(String.valueOf(avg));
            context.write(this.key, this.value);
        }
    }

    public static class TopOneMapper2 extends Mapper<Object, Text, Text, Text> {
        private final Text key = new Text();
        private final Text value = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(key.toString() + " " + value.toString() + " " + this.hashCode());
            String[] splits = value.toString().split("\t");
            this.key.set(splits[1]);
            this.value.set(splits[2] + "\t" + splits[3]);
            //System.out.println(this.key.toString() + " " + this.value);
            context.write(this.key, this.value);
        }
    }

    public static class TopOneReducer extends Reducer<Text, Text, StudentBean, NullWritable> {
        private StudentBean key = null;
        private final NullWritable value = NullWritable.get();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println(this.hashCode());
            String course = key.toString();
            List<StudentBean> beanList = new ArrayList<>();
            Iterator<Text> it = values.iterator();
            while (it.hasNext()) {
                Text t = it.next();
                String[] splits = t.toString().split("\t");
                StudentBean bean = new StudentBean();
                bean.setCourse(course);
                bean.setName(splits[0]);
                bean.setScore(splits[1]);
                beanList.add(bean);
            }
            Collections.sort(beanList);
            this.key = beanList.get(0);
            context.write(this.key, this.value);
        }
    }
}
