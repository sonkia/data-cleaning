package data.clean.core;

import data.clean.common.enums.AggrType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * 数据聚合
 * 1.指定聚合的列
 * 2.指定聚合类型
 * 3.指定的列必须能按指定的聚合类型聚合
 *
 */
@Slf4j
public class DataColumnAggregation {
    /**
     * 匹配日期字符串格式
     */
    public static String[] dateTimePatterns = {"yyyy/MM/dd HH:mm:ss","yyyy/MM/dd HH:mm", "yyyy-MM-dd HH:mm:ss", "yyyy.MM.dd HH:mm:ss"};
    //格式化日期
    public static String formatDate = "yyyy-MM-dd HH:mm:ss";
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
    //源文件编码
    public String charset;
    //聚合列的下标
    public List<Integer> columnIndexes;
    //聚合类型
    AggrType aggrType;

    public DataColumnAggregation(String expandedName, String separatorChars, String sourcePath, String targetPath,
                                 boolean existTitle, List<Integer> columnIndexes, AggrType aggrType, String charset) {
        this.expandedName = expandedName;
        this.separatorChars = separatorChars;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.existTitle = existTitle;
        this.columnIndexes = columnIndexes;
        this.aggrType = aggrType;
        this.charset = charset;
    }

    public static void main(String[] args) {

        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号数据\\";
        String sourcePath = bashPath + "step-06-分解炉及窑头数据拆分\\";
        String targetPath = bashPath + "step-07-分解炉及窑头数据按列平均聚合\\";
        List<Integer> columnIndexes = new ArrayList<>();
        columnIndexes.add(2);
        columnIndexes.add(3);
        columnIndexes.add(4);
        DataColumnAggregation dataAggregation = new DataColumnAggregation(expandedName,
                ",",
                sourcePath,targetPath,
                true,columnIndexes,AggrType.MEAN,"UTF-8");

        long start = System.currentTimeMillis();

        dataAggregation.dataAggregation();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据聚合执行耗时：" + diff + "ms");
        System.out.println("数据聚合执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 数据聚合
     */
    public void dataAggregation() {
        File file = new File(sourcePath);
        if (file.exists()) {
            File[] files = file.listFiles();
            if (null == files || files.length == 0) {
                log.warn("文件夹是空的!");
                return;
            } else {
                for (File file2 : files) {
                    if (file2.isDirectory()) {
                        continue;
                    } else {
                        if (file2.getAbsolutePath().matches(expandedName)) {
                            dataAggregation(file2);
                            System.out.println("文件:" + file2.getAbsolutePath() + " dataAggregation处理完成");
                        }
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
    }

    /**
     * 数据聚合
     * @param file 文件
     * @return
     */
    public void dataAggregation(File file) {
        //文件流
        FileInputStream fileInputStream = null;
        //读文件对象
        BufferedReader bufferedReader = null;
        //写文件对象
        FileWriter fileWriter = null;
        try {
            fileInputStream = new FileInputStream(file);

            //处理后目录不存在则创建
            File dealPathFile = new File(targetPath);
            if (!dealPathFile.exists()) {
                dealPathFile.mkdirs();
            }

            //已经存在处理后文件先删除，再创建
            File dealFile = new File(targetPath + file.getName());
            if (dealFile.exists()) {
                dealFile.delete();
            }
            dealFile.createNewFile();

            fileWriter = new FileWriter(dealFile,true);
            bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream,charset));

            String line = null;
            int counter = 0;
            List<List<Double>> lists = new LinkedList();
            StringBuffer lineData = new StringBuffer();
            while ((line = bufferedReader.readLine()) != null) {
                counter++;
                String[] strings = StringUtils.splitPreserveAllTokens(line, separatorChars);
                if (counter == 1) {
                    if (existTitle) {
                        fileWriter.write(strings[0] + separatorChars + aggrType.toString() + "\n");
                        continue;
                    }
                }

                if (strings.length <= 0) {
                    continue;
                }
                double sum = 0;
                int nullCount = 0;
                for (int i = 0; i < columnIndexes.size(); i++) {
                    String val = strings[columnIndexes.get(i)];
                    if (StringUtils.isBlank(val)) {
                        nullCount++;
                    } else {
                        sum += Double.parseDouble(val);
                    }
                }
                if (nullCount == columnIndexes.size()) {
                    fileWriter.write(strings[0] + separatorChars + "\n");
                } else {
                    fileWriter.write(strings[0] + separatorChars + sum + "\n");
                }

            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
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
                if (null != fileWriter) {
                    fileWriter.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}
