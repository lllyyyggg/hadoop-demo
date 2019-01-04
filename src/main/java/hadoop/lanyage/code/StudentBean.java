package hadoop.lanyage.code;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StudentBean implements WritableComparable<StudentBean> {
    private String course;
    private String name;
    private String score;

    public StudentBean() {
    }

    public StudentBean(String course, String name, String score) {
        this.course = course;
        this.name = name;
        this.score = score;
    }

    public String getCourse() {
        return course;
    }

    public void setCourse(String course) {
        this.course = course;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getScore() {
        return score;
    }

    public void setScore(String score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return this.course + "\t" + this.name + "\t" + this.score;
    }

    /**
     * 这个一定要返回0、-1或1中的一个
     */
    @Override
    public int compareTo(StudentBean o) {
        double difference = Double.parseDouble(this.score) - Double.parseDouble(o.score);
        if (difference == 0) {
            return 0;
        } else {
            return difference > 0 ? -1 : 1;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.course);
        dataOutput.writeUTF(this.name);
        dataOutput.writeUTF(this.score);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.course = dataInput.readUTF();
        this.name = dataInput.readUTF();
        this.score = dataInput.readUTF();
    }
}
