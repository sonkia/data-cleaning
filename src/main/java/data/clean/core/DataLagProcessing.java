package data.clean.core;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.io.*;
import java.util.Date;

/**
 * 数据滞后处理
 */
public class DataLagProcessing {
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
    //滞后周期（单位分钟） 1：加一分钟  -2：减两分钟
    public int lagCycle;
    //源文件编码
    public String charset;

    public DataLagProcessing(String expandedName, String separatorChars, String sourcePath, String targetPath, boolean existTitle, int lagCycle, String charset) {
        this.expandedName = expandedName;
        this.separatorChars = separatorChars;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.existTitle = existTitle;
        this.lagCycle = lagCycle;
        this.charset = charset;
    }

    public static void main(String[] args) {

        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号数据\\";
        String sourcePath = bashPath + "step-01-聚合处理（步长60取中位数）-s-2-m\\";
        String targetPath = bashPath + "step-02-滞后处理（加55分钟）\\";
        DataLagProcessing dataLagProcessing = new DataLagProcessing(expandedName,
                ",",
                sourcePath,targetPath,
                true,55,"UTF-8");

        long start = System.currentTimeMillis();

        dataLagProcessing.lagProcessing();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据滞后处理执行耗时：" + diff + "ms");
        System.out.println("数据滞后处理执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 数据滞后处理
     */
    public void lagProcessing() {
        File file = new File(sourcePath);
        if (file.exists()) {
            File[] files = file.listFiles();
            if (null == files || files.length == 0) {
                System.out.println("文件夹是空的!");
                return;
            } else {
                for (File file2 : files) {
                    if (file2.isDirectory()) {
                        continue;
                    } else {
                        if (file2.getAbsolutePath().matches(expandedName)) {
                            lagProcessing(file2);
                            System.out.println("文件:" + file2.getAbsolutePath() + " lagProcessing处理完成");
                        }
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
    }

    /**
     * 数据滞后处理
     * @param file 文件
     * @return
     */
    public void lagProcessing(File file) {
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
            StringBuffer lineData = new StringBuffer();
            while ((line = bufferedReader.readLine()) != null) {
                counter++;
                if (counter == 1) {
                    if (existTitle) {
                        //设置标题
                        fileWriter.write(line + "\n");
                        continue;
                    }
                }

                String[] strings = StringUtils.splitPreserveAllTokens(line,separatorChars);
                //todo notice：特殊处理
                String dateStr = strings[0].substring(1,strings[0].length() - 2);

                Date date = DateUtils.parseDate(dateStr,dateTimePatterns);
                String lagDateStr = lagDate(date,lagCycle);
                lineData.append(lagDateStr + separatorChars);
                for (int i = 1; i < strings.length; i++) {
                    if (i != strings.length - 1) {
                        lineData.append(strings[i] + separatorChars);
                    } else {
                        lineData.append(strings[i] + "\n");
                    }
                }
                fileWriter.write(lineData.toString());
                lineData.setLength(0);
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

    /**
     * 滞后日期
     * @param date
     * @param lag
     * @return
     */
    public static String lagDate(Date date, int lag){
        Date lagDate = new Date(date.getTime() + lag * 60 * 1000);
        return DateFormatUtils.format(lagDate,formatDate);
    }

}