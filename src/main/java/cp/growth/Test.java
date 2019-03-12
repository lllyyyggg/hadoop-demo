package cp.growth;


import com.lanyage.cpgrowth.datastructure.ContrastPatternTree;
import com.lanyage.cpgrowth.datastructure.ContrastPatternTreeNode;

import java.io.*;
import java.util.Base64;

public class Test {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        ContrastPatternTree tree = ContrastPatternTree.newTree("/Users/lanyage/git/cpgrowth/resources/ITEMCOUNT","/Users/lanyage/git/cpgrowth/resources/MIXEDDATASET");
        //tree.preTraverse();
        //tree = tree.merge(tree);
        //tree.preTraverse();

        ContrastPatternTreeNode root = tree.getRoot();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(root);
        byte[] strArr = bos.toByteArray();
        //System.out.println("strarr : " + strArr.length);
        String s = Base64.getEncoder().encodeToString(strArr);
        //System.out.println(s.length());
        s = GzipUtil.compress(s,"utf-8");
        //System.out.println(s.length());
        s = GzipUtil.uncompress(s, "utf-8");
        //System.out.println(s.length());

        strArr = Base64.getDecoder().decode(s);
        System.out.println("strarr : " + strArr.length);
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(strArr));
        root = (ContrastPatternTreeNode) ois.readObject();
    }

}
