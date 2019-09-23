package data.clean.business.ZhangPingPhase3;

import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.HashMap;
import java.util.Map;


public class BitName {

    public static void main(String[] args) {
        replaceBitName(getBitNameMap());
    }

    /**
     * 位号名称替换
     */
    public static void replaceBitName(Map<String,String> bitNameMap) {
        File source = new File("E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\source\\join\\source.csv");
        File target = new File("E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\source\\join\\replaceBitName.csv");

        FileInputStream fileInputStream = null;
        BufferedReader bufferedReader = null;
        FileWriter fileWriter = null;
        try {
            if (target.exists()) {
                target.delete();
            }
            target.createNewFile();
            fileInputStream = new FileInputStream(source);
            bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
            fileWriter = new FileWriter(target);
            String line = null;
            int count  = 0;
            while ((line = bufferedReader.readLine()) != null){
                count++;
                if (count == 1) {
                    String[] strings = StringUtils.splitPreserveAllTokens(line,",");
                    StringBuffer replaceHeader = new StringBuffer(strings[0]);
                    for (int i = 1; i < strings.length; i++) {
                        String name = strings[i].toUpperCase();

                        String value = bitNameMap.get(name);
                        if (null == value) {
                            replaceHeader.append(",").append(name);
                        } else {
                            replaceHeader.append(",").append(value);
                        }
                    }
                    replaceHeader.append("\n");
                    fileWriter.write(replaceHeader.toString());
                } else {
                    fileWriter.write(line + "\n");
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            try {
                if (fileInputStream != null) {
                    fileInputStream.close();
                }
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
                if (null != fileWriter) {
                    fileWriter.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 位号名map
     * @return
     */
    public static Map getBitNameMap() {
        File bitName = new File("E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号对应表.csv");
        FileInputStream fileInputStream = null;
        BufferedReader bufferedReader = null;
        //FileWriter fileWriter = null;
        try {
            fileInputStream = new FileInputStream(bitName);
            bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream,"GBK"));
            //fileWriter = new FileWriter()
            Map<String,String> bigNameMap= new HashMap<>();
            String line = null;
            while ((line = bufferedReader.readLine())!= null){
                String[] strings = StringUtils.splitPreserveAllTokens(line,",");
                bigNameMap.put(strings[1].toUpperCase(),strings[0]);
            }
            return bigNameMap;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (fileInputStream != null) {
                    fileInputStream.close();
                }
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
