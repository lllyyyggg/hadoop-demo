package cp.growth;


import com.lanyage.cpgrowth.datastructure.ContrastPatternTree;
import com.lanyage.cpgrowth.datastructure.ContrastPatternTreeNode;
import com.lanyage.cpgrowth.miner.ContrastPatternMiner;
import com.lanyage.cpgrowth.miner.ContrastTreeMerger;
import com.lanyage.cpgrowth.miner.SingleParameterChecker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class CPGrowthMR {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
        Job job = Job.getInstance(configuration, "cpgrowth");
        job.addCacheFile(new URI("cache/ITEMCOUNT#ITEMCOUNT2"));
        FileSystem.enableSymlinks();

        job.setJarByClass(CPGrowthMR.class);
        job.setMapperClass(CpGrowthMapper.class);
        job.setReducerClass(CpGrowthReducer.class);

        //job.setNumReduceTasks(3);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path inputPath = new Path("dataset");
        Path outputPath = new Path("output");
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        Job job2 = Job.getInstance(configuration, "cpgrowth2");
        job2.addCacheFile(new URI("cache/ITEMCOUNT#ITEMCOUNT2"));
        job2.setMapperClass(CpGrowthMapper2.class);
        job2.setReducerClass(CpGrowthReducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        Path pathIn = new Path("output");
        Path pathout = new Path("output2");
        if (fs.exists(pathout)) {
            fs.delete(pathout, true);
        }
        FileInputFormat.setInputPaths(job2, pathIn);
        FileOutputFormat.setOutputPath(job2, pathout);

        JobControl control = new JobControl("controller");
        ControlledJob cjob1 = new ControlledJob(job.getConfiguration());
        ControlledJob cjob2 = new ControlledJob(job2.getConfiguration());
        cjob2.addDependingJob(cjob1);

        control.addJob(cjob1);
        control.addJob(cjob2);
        Thread thread = new Thread(control);
        thread.start();
        while (!control.allFinished()) {
            Thread.sleep(1000);
        }
        System.exit(0);
    }

    public static class CpGrowthMapper extends Mapper<Object, Text, Text, Text> {
        private final Text key = new Text();
        private final Text value = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] transactionAndClassTag = value.toString().split(",");
            String classTag = transactionAndClassTag[1];
            this.key.set(classTag);
            this.value.set(value.toString());
            //System.out.println(value);
            context.write(this.key, this.value);
        }

    }

    public static class CpGrowthReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, Integer> itemCountMap = new HashMap<>();
        Text key = new Text();
        Text value = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //System.out.println("REDUCER: " + this);
            String classTag = key.toString();
            ContrastPatternTree tree = new ContrastPatternTree();
            tree.setItemCountMap(itemCountMap);
            Iterator<Text> it = values.iterator();
            while (it.hasNext()) {
                String transactionAndClassTag = it.next().toString();
                //System.out.println("ADD TREE: " + transactionAndClassTag);
                String[] transactionAndClassTagArray = transactionAndClassTag.split(",");
                String transactionString = transactionAndClassTagArray[0];
                String delim = " ";
                tree.append(transactionString, delim, classTag);
            }
            tree = new ContrastTreeMerger(tree).merge();
            //tree.preTraverse();
            List<ContrastPatternTreeNode> leaves = findLeaves(tree.getRoot());
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
                this.value.set(sb.toString().trim());
                //System.out.println("Write: " + this.value.toString());
                context.write(this.key, this.value);
            }

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

    public static class CpGrowthMapper2 extends Mapper<Object, Text, Text, Text> {
        Text key = new Text();
        Text value = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String v = value.toString().trim();
            int indexOfSpace = v.indexOf(" ");
            String k = v.substring(0, indexOfSpace);
            //System.out.println(k + " -> " + v);
            this.key.set(k);
            this.value.set(v);
            context.write(this.key, this.value);
        }
    }

    public static class CpGrowthReducer2 extends Reducer<Text, Text, Text, Text> {
        Text key = new Text();
        Text value = new Text();
        private Map<String, Integer> itemCountMap = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //System.out.println("key : " + key.toString());
            ContrastPatternTree tree = new ContrastPatternTree();
            tree.setItemCountMap(this.itemCountMap);
            for (Text text : values) {
                String transactionString = text.toString();
                ContrastPatternTreeNode head = encapsure(transactionString);
                tree.addTree(head);
            }
            //tree.preTraverse();
            ContrastPatternMiner miner = new ContrastPatternMiner(new SingleParameterChecker(0.6, 0.05));
            ContrastPatternTreeNode root = tree.getRoot();
            int rootChildrenSize = root.childrenSize();
            for (int i = 0; i < rootChildrenSize; i++) {
                miner.pureMine(root.getChild(i), 4208, 3916);
            }
        }

        private ContrastPatternTreeNode encapsure(String transactionString) {
            List<ContrastPatternTreeNode> nodes = new ArrayList<>();
            String[] contents = transactionString.split(" ");
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
