package data.clean.core;

import data.clean.common.enums.AggrType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.io.*;
import java.util.Date;

/**
 * 数据补齐
 */
public class DataFill {
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

    public DataFill(String expandedName, String separatorChars, String sourcePath, String targetPath,
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
        String sourcePath = bashPath + "step-02-滞后处理（加30分钟）\\";;
        String targetPath = bashPath + "step-03-移动平均（步长10分钟）\\";
        DataFill dataFill = new DataFill(expandedName,
                ",",
                sourcePath,targetPath,
                true,10,AggrType.MEAN,1,"UTF-8");

        long start = System.currentTimeMillis();

        dataFill.dataFill();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据补齐执行耗时：" + diff + "ms");
        System.out.println("数据补齐执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 数据补齐
     */
    public void dataFill() {
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
                            dataFill(file2);
                            System.out.println("文件:" + file2.getAbsolutePath() + " dataFill 处理完成");
                        }
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
    }

    /**
     * 数据补齐
     * @param file 文件
     * @return
     */
    public void dataFill(File file) {
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

            Date date = null;
            String str = null;
            boolean isAdd = true;
            int counter = 0;
            while ((str = bufferedReader.readLine()) != null) {
                counter++;
                if (counter == 1) {
                    if (existTitle) {
                        //写文件头
                        fileWriter.write(str + "\n");
                        continue;
                    }
                }

                String[] strings = StringUtils.splitPreserveAllTokens(str,separatorChars);

                if (strings == null || strings.length < 2) {
                    continue;
                }
                try{
                    String dateStr = strings[0];
                    String dataStr = "";
                    for (int i = 1; i < strings.length; i++) {
                        if (i == strings.length - 1) {
                            dataStr += strings[i];
                        } else {
                            dataStr += strings[i];
                            dataStr += separatorChars;
                        }
                    }
                    Date value1 = DateUtils.parseDate(dateStr,dateTimePatterns);
                    value1 = new Date(value1.getTime() - value1.getTime() % 60000);
                    if (date == null ) {
                        date = value1;
                        dateStr = DateFormatUtils.format(date,formatDate);
                        fileWriter.write(dateStr + separatorChars + dataStr + "\n");
                        continue;
                    }
                    if (isAdd) {
                        date = new Date(date.getTime() + 60000);
                    }

                    if (date.compareTo(value1) == 0 ) {
                        dateStr = DateFormatUtils.format(date,formatDate);
                        fileWriter.write(dateStr + separatorChars + dataStr + "\n");
                        isAdd = true;
                    } else if (date.compareTo(value1) > 0 ) {
                        isAdd = false;
                    } else if (date.compareTo(value1) < 0 ) {
                        while (date.compareTo(value1) <= 0) {
                            if (date.compareTo(value1) == 0 ) {
                                dateStr = DateFormatUtils.format(date,formatDate);
                                fileWriter.write(dateStr + separatorChars + dataStr + "\n");
                                break;
                            }
                            dateStr = DateFormatUtils.format(date,formatDate);
                            fileWriter.write(dateStr + separatorChars + dataStr + "\n");
                            date = new Date(date.getTime() + 60000);
                        }
                        isAdd = true;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            System.out.println("total dataline count:" + --counter);

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
