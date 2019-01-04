package hadoop.lanyage.code;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 这是一个普通的Bean
 * <p>
 * 一个对象要作为key, 那么这个对象所属的类必须实现WritableComparable接口
 * 一个对象要作为value, 那么这个对象所属的类只需要实现Writable接口即可
 */
public class ElectricityUsageBean implements WritableComparable<ElectricityUsageBean> {

    private String year;
    private long sum;

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    @Override
    public int compareTo(ElectricityUsageBean o) {
        return this.sum - o.sum == 0 ? 0 : this.sum - o.sum > 0 ? -1 : 1;
    }


    /**
     * Hadoop 采用全新的序列机制，不使用原生的Serializable接口。
     * 因为原生的接口除了序列化接口之外，还会序列化很多跟当前对象类相关的各种信息。
     */

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // 这里将对象的属性序列化出去
        dataOutput.writeUTF(year);
        dataOutput.writeLong(sum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readUTF();
        this.sum = dataInput.readLong();
    }
}
