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

public class CPGrowthMR2 {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInt("mapreduce.input.lineinputformat.linespermap", 60);
        FileSystem fs = FileSystem.get(configuration);
        Job job = Job.getInstance(configuration, "cpgrowth2");
        job.addCacheFile(new URI("cache/ITEMCOUNT#ITEMCOUNT2"));
        FileSystem.enableSymlinks();

        NLineInputFormat.setNumLinesPerSplit(job, 2200);
        job.setJarByClass(CPGrowthMR2.class);
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
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(this.tree.getRoot());
                String s = Base64.getEncoder().encodeToString(bos.toByteArray());
                //System.out.println("before : " + s.length());
                s = GzipUtil.compress(s, "utf-8");
                //System.out.println("after : " + s.length());
                s = GzipUtil.uncompress(s, "utf-8");
                byte[] bytes = Base64.getDecoder().decode(s);
                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
                ContrastPatternTreeNode root = (ContrastPatternTreeNode) ois.readObject();
                this.tree.setRoot(root);
            }catch (Exception e) {
                System.out.println(e);
            }
            //----------------------------------------------------------------------------
            List<ContrastPatternTreeNode> leaves = findLeaves(this.tree.getRoot());
            for (ContrastPatternTreeNode leaf : leaves) {
                List<ContrastPatternTreeNode> nodes = new ArrayList<>();
                while (!leaf.isRoot()) {
                    nodes.add(leaf);
                    leaf = leaf.getParent();
                }
                StringBuilder sb = new StringBuilder();
                for (int i = nodes.size() - 1; i >= 0; i--) {
                    ContrastPatternTreeNode node = nodes.get(i);
                    if (!node.isVisited()) {
                        sb.append(node.getValue()).append(" ").append(node.getC1()).append(" ").append(node.getC2()).append(" ");
                    } else {
                        sb.append(node.getValue()).append(" ").append(0).append(" ").append(0).append(" ");
                    }
                    node.setVisited();
                }
                String v = sb.toString().trim();
                int indexOfSpace = v.indexOf(" ");
                String k;
                if (indexOfSpace < 0) {
                    k = v;
                } else {
                    k = sb.substring(0, indexOfSpace);
                }
                this.key.set(k);
                this.value.set(v);
                //System.out.println(k + " -> " + v);
                context.write(this.key, this.value);
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
        //NullWritable key = NullWritable.get();
        //Text value = new Text();
        NullWritable value = NullWritable.get();
        private Map<String, Integer> itemCountMap = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //System.out.println("key -> " + key + " " + this);
            ContrastPatternTree tree = new ContrastPatternTree();
            tree.setItemCountMap(this.itemCountMap);
            for (Text value : values) {
                String transactionString = value.toString();
                ContrastPatternTreeNode head = encapsure(transactionString);
                tree.addTree(head);
            }
            ContrastPatternMiner miner = new ContrastPatternMiner(new SingleParameterChecker(0.6, 0.05));
            ContrastPatternTreeNode root = tree.getRoot();
            int rootChildrenSize = root.childrenSize();
            for (int i = 0; i < rootChildrenSize; i++) {
                try {
                    List<ContrastPatternTreeNode> patterns = miner.pureMine(root.getChild(i), 4208, 3916);
                    for (ContrastPatternTreeNode pattern : patterns) {
                        this.key.set(pattern.getValue());
                        //this.value.set(pattern.getValue());
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

        private ContrastPatternTreeNode encapsure(String transactionString) {
            List<ContrastPatternTreeNode> nodes = new ArrayList<>();
            String[] contents = transactionString.split(" ");
            if (contents.length < 3) return ContrastPatternTreeNode.NULL;
            int start = 0;
            while (start < contents.length) {
                ContrastPatternTreeNode node = ContrastPatternTreeNode.newNode(contents[start]);
                node.setC1(Integer.parseInt(contents[start + 1]));
                node.setC2(Integer.parseInt(contents[start + 2]));
                nodes.add(node);
                start += 3;
            }
            int nodeSize = nodes.size();
            if (nodeSize >= 1) {
                if (nodeSize > 1) {
                    for (int i = 1; i < nodes.size(); i++) {
                        ContrastPatternTreeNode prev = nodes.get(i - 1);
                        ContrastPatternTreeNode curr = nodes.get(i);
                        prev.addChild(curr);
                    }
                }
            }
            if (nodes.size() > 0) {
                return nodes.get(0);
            }
            return ContrastPatternTreeNode.NULL;
        }
    }

}
