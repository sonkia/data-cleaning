package data.clean.business.ZhangPingPhase3;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.io.*;
import java.util.Date;

/**
 * 漳平三期4月1日起新数据-张居宾
 */
@Slf4j
public class ZhangPingPhase3 {

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
    //格式化后目标路径
    public String targetPath;
    //是否存在标题
    public boolean existTitle;
    //源文件编码
    public String charset;

    public ZhangPingPhase3(String expandedName, String separatorChars, String sourcePath, String targetPath, boolean existTitle, String charset) {
        this.expandedName = expandedName;
        this.separatorChars = separatorChars;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.existTitle = existTitle;
        this.charset = charset;
    }

    public static void main(String[] args) {
        String expandedName = ".+\\.csv";
        String sourcePath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\原料数据\\";
        String targetPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\原料数据\\step-01-维度预处理\\";
        ZhangPingPhase3 zhangPingPhase3 =  new ZhangPingPhase3(expandedName,",",sourcePath,targetPath,true,"GB2312");

        zhangPingPhase3.dimensionRaise();
    }

    /**
     * 数据预处理：升维数据
     */
    public void dimensionRaise() {
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
                        if (file2.getName().matches(expandedName)) {
                            dimensionRaise(file2);
                            System.out.println("文件:" + file2.getAbsolutePath() + " deal处理完成");
                        }
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
    }

    /**
     * 升维数据
     * @param file
     */
    public void dimensionRaise(File file) {
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
            int is2 = 0;
            while ((line = bufferedReader.readLine()) != null) {
                counter++;
                String[] strings = StringUtils.splitPreserveAllTokens(line,separatorChars);
                if (counter == 1) {
                    if (existTitle) {
                        //设置标题
                        fileWriter.write(line + "\n");
                        continue;
                    }
                }
                if (++is2 == 2) {
                    is2 = 0;
                    for (int i = 2; i < strings.length; i++) {
                        if (i != strings.length - 1) {
                            lineData.append(strings[i] + separatorChars);
                        } else {
                            lineData.append(strings[i] + "\n");
                        }
                    }
                    fileWriter.write(lineData.toString());
                    lineData.setLength(0);
                } else {
                    Date date = DateUtils.parseDate(strings[0],dateTimePatterns);
                    String dateStr = DateFormatUtils.format(date,formatDate);
                    lineData.append(dateStr + separatorChars);
                    for (int i = 2; i < strings.length; i++) {
                        lineData.append(strings[i] + separatorChars);
                    }
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

    /**
     * 设置标题
     * @param fileWriter
     * @param strings
     */
    private void setTitle(FileWriter fileWriter,String[] strings) throws IOException {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append(strings[0] + ",");

        String base = "三线1#生料磨";
        for (int i = 2; i < strings.length; i++) {
            stringBuffer.append(base + "_" + strings[i] + separatorChars );
        }

        base = "三线2#生料磨";
        for (int i = 2; i < strings.length; i++) {
            if (i == strings.length - 1) {
                stringBuffer.append(base + "_" + strings[i] + "\n" );
            } else {
                stringBuffer.append(base + "_" + strings[i] + separatorChars );
            }
        }
        fileWriter.write(stringBuffer.toString());
        stringBuffer.setLength(0);
    }
}
