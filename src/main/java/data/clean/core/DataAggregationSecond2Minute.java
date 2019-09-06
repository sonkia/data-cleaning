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
 * 秒级数据聚合成分钟
 * 例如：
 * 00:00:00--00:00:59范围数据，按聚合类型计算，时间打标为00:00:00
 * 00:01:00--00:01:59范围数据，按聚合类型计算，时间打标为00:01:00
 */
@Slf4j
public class DataAggregationSecond2Minute {
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
    //步长
    public int step;
    //聚合类型
    AggrType aggrType;
    //第一个有效列
    public int firstCol;

    public DataAggregationSecond2Minute(String expandedName, String separatorChars, String sourcePath, String targetPath,
                                        boolean existTitle, int step, AggrType aggrType, int firstCol, String charset) {
        this.expandedName = expandedName;
        this.separatorChars = separatorChars;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.existTitle = existTitle;
        this.step = step;
        this.aggrType = aggrType;
        this.firstCol = firstCol;
        this.charset = charset;
    }

    public static void main(String[] args) {

        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号数据\\";
        String sourcePath = bashPath;
        String targetPath = bashPath + "step-01-聚合处理（步长60取中位数）-s-2-m\\";
        DataAggregationSecond2Minute dataAggregation = new DataAggregationSecond2Minute(expandedName,
                ",",
                sourcePath,targetPath,
                true,60,AggrType.MEDIAN,2,"GBK");

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
            int count = 0;
            List<List<Double>> lists = new LinkedList();
            StringBuffer lineData = new StringBuffer();
            while ((line = bufferedReader.readLine()) != null) {

                if (counter == 0) {
                    counter++;
                    if (existTitle) {
                        //设置标题
                        if (firstCol == 1) {
                            fileWriter.write(line + "\n");
                        } else if (firstCol > 1) {
                            int pos = line.indexOf(separatorChars);
                            String title = line.substring(pos + 1,line.length());
                            fileWriter.write(title + "\n");
                        } else {
                            throw new RuntimeException("firstCol < 1  :" + firstCol);
                        }
                        continue;
                    }
                }

                String[] strings = StringUtils.splitPreserveAllTokens(line,separatorChars);
                if (strings.length <= 0) {
                    continue;
                }

                count++;
                try{
                    String dateStr = strings[firstCol - 1];
                    if (count == 1 || count % step == 1) {
                        lineData.append(dateStr);
                    }
                    for (int i = firstCol; i < strings.length; i++) {
                        String val = strings[i];
                        List<Double> values;
                        //首次初始化值
                        if (count == 1) {
                            values = new ArrayList<>();
                            lists.add(values);
                        } else {
                            //不是首次，获取缓存值
                            values = lists.get(i - firstCol);
                            //更新缓存值
                            if (StringUtils.isNotBlank(val)) {
                                double valDouble = Double.parseDouble(val);
                                values.add(valDouble);
                            } else {
                                values.add(null);
                            }

                            if (count % step == 0) {
                                String aggr = DataAggrFunction.aggr(values,aggrType);
                                lineData.append(separatorChars + aggr);
                                values.clear();
                            }
                        }

                    }

                    if (count % step == 0) {
                        lineData.append("\n");
                        fileWriter.write(lineData.toString());
                        lineData.setLength(0);
                    }

                    if (count % 100000 == 0) {
                        System.out.println("处理条数：" + count);
                        System.out.println(DateFormatUtils.format(new Date(),formatDate));
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            System.out.println("total dataline count:" + count);

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
