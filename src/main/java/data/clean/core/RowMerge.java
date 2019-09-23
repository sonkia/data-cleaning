package data.clean.core;

import data.clean.common.enums.AggrType;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 文件行合并
 * 指定开始合并的行
 * 指定n行合并为一行
 * 注：默认第一类是时间列 n行为同一个时间来合并行，以后再优化处理
 */
public class RowMerge {

    //扩展名
    public String expandedName;
    //分隔符
    public String separatorChars;
    //原路径
    public String sourcePath;
    //目标路径
    public String targetPath;
    //是否存在标题
    public boolean existTitle;
    //开始行
    public int startRow;
    //合并单位 n行
    public int mergeRowCount;
    //聚合类型
    public AggrType aggrType;
    //源文件编码
    public String charset;

    public RowMerge(String expandedName, String separatorChars, String sourcePath, String targetPath,int startRow,
                    int mergeRowCount,AggrType aggrType,boolean existTitle, String charset) {
        this.expandedName = expandedName;
        this.separatorChars = separatorChars;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.startRow = startRow;
        this.mergeRowCount = mergeRowCount;
        this.aggrType = aggrType;
        this.existTitle = existTitle;
        this.charset = charset;
    }

    /**
     * 行合并
     */
    public void rowMerge() {
        File file = new File(sourcePath);
        if (file.exists()) {
            File[] files = file.listFiles();
            if (null == files || files.length == 0) {
                System.out.println("文件夹是空的!");
                return;
            } else {
                int mergeTimes = 0;
                for (File file2 : files) {
                    mergeTimes++;
                    if (file2.isDirectory()) {
                        continue;
                    } else {
                        rowMerge(file2);
                        System.out.println("文件:" + file2.getAbsolutePath() + " 处理完成");
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
    }

    public void rowMerge(File file) {
        //文件流
        FileInputStream stream = null;
        //读文件对象
        BufferedReader reader = null;
        //写文件对象
        FileWriter writer = null;
        File frontFile = null;
        try {
            stream = new FileInputStream(file);
            String pathFile = targetPath + file.getName();
            File targetPathFile = new File(targetPath);
            File targetFile = new File(pathFile);
            if(!targetPathFile.exists()){
                targetPathFile.mkdirs();
            }

            if(targetFile.exists()){
                targetFile.delete();
            }
            targetFile.createNewFile();
            writer = new FileWriter(targetFile,true);
            reader = new BufferedReader(new InputStreamReader(stream,charset));

            List<List<Double>> cacheData = new ArrayList<>();
            String dataStr = "";
            int rowCount = 0;
            String str = null;
            int counter = 0;
            while ((str = reader.readLine()) != null) {
                counter++;
                String[] strings = StringUtils.splitPreserveAllTokens(str,separatorChars);
                if (counter == 1) {
                    for (int i = 1; i < strings.length; i++) {
                        List<Double> doubles = new ArrayList<>();
                        cacheData.add(doubles);
                    }
                }
                // 未到开始行，数据直接写入
                if (counter < startRow) {
                    writer.write(str + "\n");
                } else {
                    if (rowCount == mergeRowCount) {
                        rowCount = 1;
                        StringBuffer stringBuffer = new StringBuffer(dataStr);
                        for (int i = 0; i < cacheData.size(); i++) {
                            List<Double> doubles = cacheData.get(i);
                            String res = DataAggrFunction.aggr(doubles,aggrType);
                            stringBuffer.append(separatorChars).append(res);
                            doubles.clear();
                        }
                        writer.write(stringBuffer.toString() + "\n");
                    } else {
                        rowCount++;
                    }
                    dataStr = strings[0];
                    for (int i = 0; i < strings.length - 1; i++) {
                        List<Double> doubles = cacheData.get(i);
                        String val = strings[i + 1];
                        if (StringUtils.isBlank(val)) {
                            doubles.add(null);
                        } else {
                            doubles.add(Double.parseDouble(val));
                        }
                    }
                }
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stream != null) {
                    stream.close();
                }
                if (reader != null) {
                    reader.close();
                }
                if (null != writer) {
                    writer.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (frontFile != null) {
            frontFile.delete();
        }
    }

}
