package data.clean.core;

import data.clean.common.enums.IntervalType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * 数据采样
 * 1.指定时间粒度
 *
 */
@Slf4j
public class DataSampling {
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
    //间隔类型
    IntervalType intervalType;

    public DataSampling(String expandedName, String separatorChars, String sourcePath, String targetPath,
                        boolean existTitle, IntervalType intervalType, String charset) {
        this.expandedName = expandedName;
        this.separatorChars = separatorChars;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.existTitle = existTitle;
        this.intervalType = intervalType;
        this.charset = charset;
    }


    /**
     * 数据采样
     */
    public void dataSampling() {
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
                            dataSampling(file2);
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
     * 数据采样
     * @param file 文件
     * @return
     */
    public void dataSampling(File file) {
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
            while ((line = bufferedReader.readLine()) != null) {
                counter++;
                if (counter == 1) {
                    fileWriter.write(line + "\n");
                    continue;
                }

                String[] strings = StringUtils.splitPreserveAllTokens(line,separatorChars);
                if (strings.length <= 0) {
                    continue;
                }
                count++;
                try{
                    //todo notice：特殊处理 因为日期字符串多了一个空格和前后引号
                    //String dateStr = strings[0].substring(1,strings[0].length() - 2);
                    String dateStr = strings[0];
                    if (DateUtils.parseDate(dateStr,dateTimePatterns).getTime() % 3600000 == 0) {
                        StringBuffer stringBuffer = new StringBuffer(dateStr);
                        for (int i = 1; i < strings.length; i++) {
                            stringBuffer.append(separatorChars).append(strings[i]);
                        }
                        fileWriter.write(stringBuffer.toString() + "\n");
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
