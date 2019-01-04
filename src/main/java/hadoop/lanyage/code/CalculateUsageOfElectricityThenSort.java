package hadoop.lanyage.code;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

import java.io.IOException;

public class CalculateUsageOfElectricityThenSort {
    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Calculate");

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Sort");

        JobControl jobControl = new JobControl("CalculateAndSort");
        ControlledJob controlledJob1 = new ControlledJob(job.getConfiguration());
        ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());

        controlledJob2.addDependingJob(controlledJob1);

        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        Thread thread = new Thread(jobControl);
        thread.start();

        while (!jobControl.allFinished()) {
            Thread.sleep(1000);
        }
        System.exit(0);

    }
    public static class SortMapper extends Mapper {

    }
    public static class SortReducer extends Reducer {

    }
}
