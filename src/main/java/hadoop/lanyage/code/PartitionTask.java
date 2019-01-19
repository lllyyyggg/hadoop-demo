package hadoop.lanyage.code;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class PartitionTask {
    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "helloworld");

        //设置Partitioner, 数据会按照一定的规则分配到不同的Reducer节点上, ReduceTask默认是10个
        job.setPartitionerClass(FivePartitioner.class);
        job.setNumReduceTasks(10);
    }
}
