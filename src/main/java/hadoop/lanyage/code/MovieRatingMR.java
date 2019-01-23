package hadoop.lanyage.code;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reduce Join
 */
public class MovieRatingMR {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "reduce join");
    }

    private static class MovieRatingMapper extends Mapper<Object, Text, Text, Text> {

        private Text key = new Text();
        private NullWritable value = NullWritable.get();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();

            if (fileName.equals("movie.txt")) {
                //1,Toy Story (1995),Animation|Children's|Comedy
                //<1, "movie#Toy Story (1995),Animation|Children's|Comedy">
            } else {
                //1,1193,5,978300760
                //<1193, "movie#1,5,978300760">
            }
        }
    }

    private static class MovieRatingReducer extends Reducer<Text, Text, Text, Text> {
        // 容易产生OOM
        private List<String> movieList = new ArrayList<>();
        private List<String> ratingList = new ArrayList<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                if (value.toString().startsWith("movie#")) {
                    movieList.add(value.toString());
                } else {
                    ratingList.add(value.toString());
                }
            }
            // 此处应该是2层循环，根据一条条件把movie和rating拼起来
        }
    }


}
