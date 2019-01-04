package hadoop.lanyage.code;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.IOException;

public class FivePartitioner extends Partitioner<Object, Object> {
    @Override
    public int getPartition(Object o, Object o2, int numReducers) {
        int value = o.hashCode();
        return value % 5 == 0 ? 0 : 1;
    }

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "halo");
        job.setPartitionerClass(FivePartitioner.class);
        job.setNumReduceTasks(2);
    }
}
