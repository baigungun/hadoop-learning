package com.people.dataseg.seg2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.people.dataseg.utils.SegContants;
import com.people.dataseg.utils.SegMathFunc;

//deal one seg file once.
public class Seg2Reducer extends Reducer<Text, Text, Text, Text> {

    /** the map of cnf info. */
    Map<String, Map<String, String>> cnfMap = new HashMap<String, Map<String, String>>();
    Map<String, String> channelMap = new HashMap<String, String>();
    /** the path of cnf. */
    private String segType = "";
    private String prefix = "";
    private String orgCnf = "";

    private String segTypeMoney = "money";
    private String segTypeFreq = "freq";

    static int SEGCNT = SegContants.SEGCOUNT;

    // 不同接口的频次计数结果
    Map<String, Map<Long, Integer>> rltMap = new TreeMap<String, Map<Long, Integer>>();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        segType = conf.get("segType");
        prefix = conf.get("cnfPrefix");
        orgCnf = conf.get("orgCnf");
        System.out.println("reduce segType = " + segType + ",cnfPrefix=" + prefix + "orgCnf=" + orgCnf);
        readCnfInfo(context);
    }

    // 解析分段文件，并将文件存储为MAP格式
    public void parseCnf(Path inFile, FileSystem fs, String type) throws IOException {
        String s = "";
        Map<String, String> infoMap = new HashMap<String, String>();
        FSDataInputStream fin = fs.open(inFile);
        BufferedReader input = new BufferedReader(new InputStreamReader(fin, "UTF-8"));

        try {
            // 处理当前文件数据
            while ((s = input.readLine()) != null) {
                String[] items = s.split("\t");
                String name = items[0];
                String code = items[1];
                String seg = items[2].trim();
                channelMap.put(code, name);
                infoMap.put(code, seg);
            }
            cnfMap.put(type, infoMap);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 释放
            if (input != null) {
                input.close();
                input = null;
            }
            if (fin != null) {
                fin.close();
                fin = null;
            }
        }
    }

    /*
     * 读取指定HDFS路径下所有文件的数据.
     */
    public void readCnfInfo(Context context) throws IOException {
        System.out.println("begin cnfMap is : " + cnfMap);
        try {
            FileSystem fs = FileSystem.get(URI.create(prefix), context.getConfiguration());
            // 读取文件列表 ,依次处理每个文件
            Path intfilePath = new Path(orgCnf);
            if (segTypeMoney.equals(segType)) {
                parseCnf(intfilePath, fs, segTypeMoney);
            } else if (segTypeFreq.equals(segType)) {
                parseCnf(intfilePath, fs, segTypeFreq);
            }
            System.out.println("result map is " + cnfMap);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<String, String> orgsegMap = cnfMap.get(segType);
        // 没有任何数据的时候，直接采用原始分段
        if (rltMap.isEmpty()) {
            System.out.println("seg file is null.");
            for (Map.Entry<String, String> entry : orgsegMap.entrySet()) {
                String orgint = entry.getKey();
                String orgName = channelMap.get(orgint);
                String orgcontent = entry.getValue();
                context.write(new Text(orgName + "\t" +orgint + "\t" + orgcontent), new Text());
            }

        } else {
            Map<String, ArrayList<Long>> intSegNodeMap = SegMathFunc.getSegNode(rltMap, SEGCNT, segType);
            // 组装成字符串并写入文件 intSegNodeMap
            for (Map.Entry<String, ArrayList<Long>> entry : intSegNodeMap.entrySet()) {
                String channel = entry.getKey();
                if (orgsegMap.containsKey(channel)) {
                    String cName = channelMap.get(channel);
                    String orgSegStr = orgsegMap.get(channel);
                    String outString = getFormatStr(entry.getValue(), orgSegStr);
                    String rltString = cName + "\t"+ channel + "\t" + outString;
                    context.write(new Text(rltString), new Text());
                }
            }
        }

        // 释放数据
        if (!cnfMap.isEmpty()) {
            cnfMap.clear();
        }
        
        if(!channelMap.isEmpty()){
            channelMap.clear();
        }

    }

    // 传入分段节点[3,6,7,9]，返回字符串[0,22);[22,43);[43,64);[64,85);[85,~)格式
    // 如果分段结果不合理，将原始分段结果返回
    private static String getFormatStr(ArrayList<Long> segNode, String orgSegStr) {
        Integer len = segNode.size();
        if (len < SEGCNT) {
            return orgSegStr;
        }

        String rlt = "[0,";
        String endString = "~)";
        for (int i = 0; i < len; i++) {
            Long node = segNode.get(i);
            rlt += node.toString() + ");[" + node.toString() + ",";
        }

        rlt += endString;
        return rlt;
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String channel_type = key.toString();
        String[] tokens = channel_type.split("\t");
        String channel = tokens[0];// pos,atm

        Map<Long, Integer> lMap = new TreeMap<Long, Integer>();
        for (Text val : values) {
            String line = val.toString();
            if (line.trim().isEmpty())
                continue;
            String[] infos = line.trim().split("\t");
            if (infos.length < 2)
                continue;
            Long freq = Long.parseLong(infos[0]);
            int cnt = Integer.parseInt(infos[1]);
            lMap.put(freq, cnt);

        }// end for
        rltMap.put(channel, lMap);
    }// end reduce

    public static void main(String[] args) {

        ArrayList<Long> node = new ArrayList<Long>();
        node.add(3L);
        node.add(5L);
        node.add(7L);
        // node.add(10L);
        String rltString = getFormatStr(node, "orgstr");
        System.out.println(rltString);

    }

}
