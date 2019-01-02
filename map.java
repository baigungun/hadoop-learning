package com.people.prepare.feature;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.prepare.common.RuleUtils;
import com.prepare.util.MRUtil;
import com.prepare.util.ModelConstants;

public class FeatureMapper extends Mapper<Object, Text, Text, Text> {

    Map<String, Map<String, String>> actionCnfMap = new HashMap<String, Map<String, String>>();
    ArrayList<Integer> riskCodeList = new ArrayList<Integer>();
    String pattern = "^0+$";
    Pattern r = Pattern.compile(pattern);

    private String prefixPath = "";
    private String actionPath = "";

    protected void setup(Context context) throws IOException, InterruptedException {
       //读取hdfs上文件并进行解析
        Configuration conf = context.getConfiguration();
        //从job中传输配置路径
        prefixPath = conf.get("prefixCnf");
        actionPath = conf.get("actionCnf");
        readCnfInfo(context);

        // risk保存高风险的失败代码
        String riskCode = ModelConstants.RESP_CODE_RISK;
        String[] riskCodes = riskCode.trim().split(",");
        int tmpRiskCode = 0;
        for (String code : riskCodes) {
        	tmpRiskCode = RuleUtils.str2int(code);
        	riskCodeList.add(tmpRiskCode);
        }
    }

    public void readCnfInfo(Context context) throws IOException {
        try {
            FileSystem fs = FileSystem.get(URI.create(prefixPath), context.getConfiguration());
            Path actionFile = new Path(actionPath);
            parseActionCnf(actionFile, fs); // 动作
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 解析动作配置文件
    public void parseActionCnf(Path inFile, FileSystem fs) throws IOException {
        // 1;;pos_consume;;22,66;;5;;0.5;;01
        String s = "";
        FSDataInputStream fin = fs.open(inFile);
        BufferedReader input = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
        Map<String, String> posActionCnfMap = new HashMap<String, String>();
        Map<String, String> atmActionCnfMap = new HashMap<String, String>();

        try {
            // 1 "\t" pos_consume "\t" 22,66 "\t" 5 "\t" 0.5
            while ((s = input.readLine()) != null) {
                String[] items = s.split(ModelConstants.CONF_SEPERATE); // "\t"分割
                if (items.length < 7)
                    continue;
                String action = items[1];// 动作类型名称
                String channel =items[6];
                String actionNum = items[2];// 动作编号

                if(channel.equals(ModelConstants.CHANNEL_POS)){
                	if (actionNum.contains(",")) {
                        String[] aNums = actionNum.split(",");
                        for (String aNum : aNums) {
                        	posActionCnfMap.put(aNum, action);
                        }
                    } else {
                        // {"22":"pos_consume"}
                    	posActionCnfMap.put(actionNum, action); // 动作编号 动作类型名称
                    }
                }
                if(channel.equals(ModelConstants.CHANNEL_ATM)){
                	if (actionNum.contains(",")) {
                        String[] aNums = actionNum.split(",");
                        for (String aNum : aNums) {
                        	atmActionCnfMap.put(aNum, action);
                        }
                    } else {
                        // {"22":"pos_consume"}
                    	atmActionCnfMap.put(actionNum, action); // 动作编号 动作类型名称
                    }
                }
            }
            actionCnfMap.put(ModelConstants.CHANNEL_POS, posActionCnfMap);
            actionCnfMap.put(ModelConstants.CHANNEL_ATM, atmActionCnfMap);
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
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
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        if (value == null)
            return;
        String line = value.toString();
        if (line.trim().isEmpty())
            return;
        String[] tokens = line.split(ModelConstants.LOG_SEPARATE);
        if (tokens.length < ModelConstants.LOG_PRE_LEN) {
            return;
        }

        String pan = tokens[0];// 卡号 POS & ATM
        String timeStamp = tokens[1];// 时间戳 POS & ATM
     
        if (!ModelConstants.CARD_TYPE_DEBIT.equals(cardType))// 卡类型：保存借记卡 ;过滤掉信用卡
            return;
        double money = RuleUtils.str2double(moneyStr);// 交易金额转换为double类型

        String actionName = null;
        try {
			actionName = actionCnfMap.get(channel).get(action);// 动作类型
		} catch (Exception e) {
			e.printStackTrace();
		}

        // POS
        if (channel.equals(ModelConstants.CHANNEL_POS)) {
            String f0 = "0";// 0.money > mcc MaxMoney
            String f1 = "0";//          
            String f26 = "0";// 半小时:退货次数
            String f27 = f11;// 半小时:
      
           
            }
          
            String outPosString = MRUtil.appendString(timeStamp, actionName, f0, f1, f2, f3, f4, f5, f6, f7, f8, f9,
                            f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, f23, f24, f25, f26, f27, f28);
            context.write(new Text(channel + ModelConstants.LOG_SEPARATE + pan), new Text(outPosString));
        }
      
    }

    protected void cleanup(Context context) throws IOException {
        // 释放数据
        if (!riskCodeList.isEmpty())
            riskCodeList.clear();

        if (!actionCnfMap.isEmpty())
            actionCnfMap.clear();
    }
}
