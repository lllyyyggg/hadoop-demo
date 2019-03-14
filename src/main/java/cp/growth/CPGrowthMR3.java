package cp.growth;

import com.lanyage.cpgrowth.datastructure.ContrastPatternTree;
import com.lanyage.cpgrowth.datastructure.ContrastPatternTreeNode;
import com.lanyage.cpgrowth.miner.ContrastPatternMiner;
import com.lanyage.cpgrowth.miner.ContrastTreeMerger;
import com.lanyage.cpgrowth.miner.SingleParameterChecker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.*;

public class CPGrowthMR3 {


    public static void main(String[] args) throws Exception {
        System.out.println("温馨提示: 开始MapReduce程序");
        System.out.println("温馨提示: 请将指定的ITEMCOUNT文件和MIXEDDATASET文件放入指定的路径下,分别是HDFS的cache/和dataset/目录");
        //verifyInputArgs(args);
        Configuration configuration = new Configuration();
        configuration.setInt("mapreduce.input.lineinputformat.linespermap", 60);
        FileSystem fs = FileSystem.get(configuration);
        Job job = Job.getInstance(configuration, "cpgrowth2");
        job.addCacheFile(new URI("cache/ITEMCOUNT#ITEMCOUNT2"));
        FileSystem.enableSymlinks();

        NLineInputFormat.setNumLinesPerSplit(job, 4000);
        job.setJarByClass(CPGrowthMR3.class);
        job.setInputFormatClass(NLineInputFormat.class);
        job.setMapperClass(CpGrowthMapper.class);
        job.setReducerClass(CpGrowthReducer.class);

        Path inputPath = new Path("dataset");
        Path outputPath = new Path("output");
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        //job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        //job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }

    public static class CpGrowthMapper extends Mapper<Object, Text, Text, Text> {
        private Map<String, Integer> itemCountMap = new HashMap<>();
        Text key = new Text();
        Text value = new Text();
        ContrastPatternTree tree = new ContrastPatternTree();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] transactionAndClassTagArray = value.toString().split(",");
            if (transactionAndClassTagArray.length == 2) {
                String transactionString = transactionAndClassTagArray[0];
                String delim = " ";
                String classTag = transactionAndClassTagArray[1];
                //System.out.println("ADD -> " + value + " -> " + this);
                tree.append(transactionString, delim, classTag);
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("ITEMCOUNT2")));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] itemAndCount = line.split(" ");
                    String item = itemAndCount[0];
                    Integer value = Integer.parseInt(itemAndCount[1]);
                    this.itemCountMap.put(item, value);
                }
                br.close();
            }
            tree.setItemCountMap(this.itemCountMap);
        }


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            this.tree = new ContrastTreeMerger(this.tree).merge();
            // 思路 此处将节点直接进行序列化为字节数组，并且进行值进行传播
            try {
                ContrastPatternTreeNode root = this.tree.getRoot();
                if (root.childrenSize() > 0) {
                    for (ContrastPatternTreeNode rootChild : root.getChildren()) {
                        ContrastPatternTreeNode newRootChild = rootChild.deepCopy();
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(bos);
                        oos.writeObject(newRootChild);
                        String sequenceString = Base64.getEncoder().encodeToString(bos.toByteArray());
                        //System.out.println("before : " + sequenceString.length());
                        sequenceString = GzipUtil.compress(sequenceString, "utf-8");
                        //System.out.println("after : " + sequenceString.length());
                        this.key.set(newRootChild.getValue());
                        this.value.set(sequenceString);
                        context.write(this.key, this.value);
                    }
                }
            } catch (Exception e) {
                System.out.println(e);
            }
            super.cleanup(context);
        }

        private List<ContrastPatternTreeNode> findLeaves(ContrastPatternTreeNode root) {
            List<ContrastPatternTreeNode> result = new ArrayList<>();
            LinkedList<ContrastPatternTreeNode> stack = new LinkedList<>();
            stack.addLast(root);
            while (!stack.isEmpty()) {
                ContrastPatternTreeNode node = stack.pollLast();
                if (node.childrenSize() == 0) {
                    result.add(node);
                }
                if (node.childrenSize() > 0) {
                    for (int j = node.childrenSize() - 1; j >= 0; j--) {
                        stack.addLast(node.getChild(j));
                    }
                }
            }
            return result;
        }
    }

    public static class CpGrowthReducer extends Reducer<Text, Text, Text, NullWritable> {
        Text key = new Text();
        NullWritable value = NullWritable.get();
        private Map<String, Integer> itemCountMap = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //System.out.println("key -> " + key + " " + this);
            ContrastPatternTree tree = new ContrastPatternTree();
            tree.setItemCountMap(this.itemCountMap);
            for (Text value : values) {
                String sequenceString = value.toString();
                sequenceString = GzipUtil.uncompress(sequenceString, "utf-8");
                byte[] bytes = Base64.getDecoder().decode(sequenceString);
                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
                ContrastPatternTreeNode recover = null;
                try {
                    recover = (ContrastPatternTreeNode) ois.readObject();
                } catch (ClassNotFoundException e) {
                    System.out.println(e);
                }
                tree.addTree(recover);
            }

            ContrastPatternMiner miner = new ContrastPatternMiner(new SingleParameterChecker(0.6, 0.05));
            ContrastPatternTreeNode root = tree.getRoot();
            int rootChildrenSize = root.childrenSize();
            for (int i = 0; i < rootChildrenSize; i++) {
                try {
                    List<ContrastPatternTreeNode> patterns = miner.pureMine(root.getChild(i), 4208, 3916);
                    for (ContrastPatternTreeNode pattern : patterns) {
                        this.key.set(pattern.getValue());
                        context.write(this.key, this.value);
                    }
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("ITEMCOUNT2")));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] itemAndCount = line.split(" ");
                    String item = itemAndCount[0];
                    Integer value = Integer.parseInt(itemAndCount[1]);
                    this.itemCountMap.put(item, value);
                }
                br.close();
            }
        }
    }

}
