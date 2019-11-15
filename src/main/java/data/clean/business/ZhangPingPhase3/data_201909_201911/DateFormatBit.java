package data.clean.business.ZhangPingPhase3.data_201909_201911;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
public class DateFormatBit {

    /**
     * 匹配日期字符串格式
     */
    public static String[] dateTimePatterns = {"yyyy/MM/dd HH:mm:ss","yyyy/MM/dd HH:mm", "yyyy-MM-dd HH:mm:ss", "yyyy.MM.dd HH:mm:ss"};
    //扩展名
    public String expandedName;
    //基础路径
    public String bashPath;
    //原路径
    public String sourcePath;
    //目标路径
    public String targetPath;
    //是否存在标题
    public boolean existTitle;


    public DateFormatBit(String expandedName, String bashPath, String sourcePath, String targetPath, boolean existTitle) {
        this.expandedName = expandedName;
        this.bashPath = bashPath;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.existTitle = existTitle;
    }

    /**
     *
     */
    public void formatDirectory() {
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
                            format(file2.getName());
                            System.out.println("文件:" + file2.getAbsolutePath() + " format处理完成");
                        }
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
    }

    /**
     * 格式化数据
     * @param fileName 文件
     * @return
     */
    public void format(String fileName) {
        //BufferedReader是可以按行读取文件
        FileInputStream inputStream = null;
        BufferedReader bufferedReader = null;
        FileWriter fileWritter= null;
        try {
            inputStream = new FileInputStream(sourcePath + fileName);
            String pathFile = targetPath + fileName;
            File file = new File(targetPath);
            File file1 = new File(pathFile);
            if(!file.exists()){
                file.mkdirs();
            }
            if(file1.exists()){
                file1.delete();
            }
            file1.createNewFile();
            fileWritter = new FileWriter(pathFile,true);

            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String str = null;
            int counter = 0;
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            while ((str = bufferedReader.readLine()) != null) {
                if (counter == 0) {
                    counter++;
                    if (existTitle) {
                        //写文件头
                        fileWritter.write(str + "\n");
                        continue;
                    }
                }
                String[] strings = StringUtils.splitPreserveAllTokens(str,",");
                if (strings == null || strings.length < 2) {
                    continue;
                }
                try{
                    Date value1 = DateUtils.parseDate(strings[0],dateTimePatterns);
                    //按分钟取整
                    value1 = new Date(value1.getTime() - value1.getTime() % 60000);
                    StringBuffer stringBuffer = new StringBuffer(simpleDateFormat.format(value1));
                    for (int i = 1; i < strings.length; i++) {
                        stringBuffer.append(",").append(strings[i]);
                    }
                    fileWritter.write(stringBuffer.toString() + "\n");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        } catch (FileNotFoundException e) {
            log.error("file not found");
        } catch (RuntimeException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("文件读取异常");
        } finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
                if (null != fileWritter) {
                    fileWritter.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}