package hadoop.lanyage.code;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 范型<KEY_TYPE, VALUE_TYPE>
 */
public class CoursePartitioner extends Partitioner<StudentBean, NullWritable> {

    @Override
    public int getPartition(StudentBean studentBean, NullWritable nullWritable, int numPartitions) {
        return PartitionEnum.get(studentBean.getCourse());
    }
}
