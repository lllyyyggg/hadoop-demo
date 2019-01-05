package hadoop.lanyage.code;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CoursePartitionerII extends Partitioner<IntWritable, StudentBean> {

    @Override
    public int getPartition(IntWritable key, StudentBean value, int numPartitions) {
        return PartitionEnum.get(value.getCourse());
    }
}

