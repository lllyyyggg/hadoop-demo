package hadoop.lanyage.code;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class PartitionTask {
    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "helloworld");
        job.setPartitionerClass(FivePartitioner.class);
        job.setNumReduceTasks(10);
    }
}
