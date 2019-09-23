package data.clean.business.ZhangPingPhase3;

import java.io.*;

public class FileDealDemo {

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

    public FileDealDemo(String expandedName, String separatorChars, String sourcePath, String targetPath, boolean existTitle, String charset) {
        this.expandedName = expandedName;
        this.separatorChars = separatorChars;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.existTitle = existTitle;
        this.charset = charset;
    }

    public static void main(String[] args) {
        String expandedName = ".+\\.csv";
        String sourcePath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号数据\\step-01-位号数据：秒级数据聚合成分钟级数据\\";
        FileDealDemo compareFile =  new FileDealDemo(expandedName,",",sourcePath,null,true,"UTF-8");

        compareFile.comp();
    }

    /**
     * 数据预处理 5184002
     */
    public void comp() {
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
                            comp(file2);
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
     *
     * @param file
     */
    public void comp(File file) {
        //文件流
        FileInputStream fileInputStream = null;
        //读文件对象
        BufferedReader bufferedReader = null;
        try {
            fileInputStream = new FileInputStream(file);
            bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream,charset));
            String line = null;
            int counter = 0;
            while ((line = bufferedReader.readLine()) != null) {
                counter++;
//                if (counter <= 1000) {
//                    System.out.println(line);
//                } else {
//                    break;
//                }
            }
            System.out.println(counter);
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
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
