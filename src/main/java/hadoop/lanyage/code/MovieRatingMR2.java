package hadoop.lanyage.code;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Map Join
 */
public class MovieRatingMR2 {
    public static void main(String[] args) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job-name");

        // 缓存普通文件到task运行的节点
        URI uri = new URI("hdfs://hadoop1:9000/output/movie");
        job.addCacheFile(uri);
    }

    private static class MovieRatingMapper2 extends Mapper<Object, Text, Text, IntWritable> {
        private Map<String, String> movieMap = new HashMap<>();

        // 将缓存到本地的文件存储到map中去
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] pathFiles = context.getLocalCacheFiles();
            String path = pathFiles[0].toUri().toString();
            BufferedReader br = new BufferedReader(new FileReader(path));
            String readLine;

            while ((readLine = br.readLine()) != null) {
                System.out.println(readLine);
                String[] splits = readLine.split(",");
                String movieId = splits[0];
                String movieNameAndType = splits[1] + "\t" + splits[2];
                movieMap.put(movieId, movieNameAndType);
            }
            br.close();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //  读取rating.dat文件，然后从 <1, "1001 89">传给reducer
        }
    }
    private static class MovieRatingReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // <1, movieName 10> 写入结果文件
        }
    }
}
