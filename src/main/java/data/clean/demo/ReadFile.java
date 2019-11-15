package data.clean.demo;

import java.io.*;
import java.util.HashSet;

public class ReadFile {
    public static void main(String[] args) {
        File file = new File("E:\\work\\天数\\数据清洗\\红狮数据\\漳平红狮三期201909-201911数据\\清洗\\step-00-所有位号数据合并");
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
                        readFile(file2);
                        System.out.println("文件:" + file2.getAbsolutePath() + " 处理完成");
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
    }

    public static void readFile(File file){
        //文件流
        FileInputStream fileInputStream = null;
        //读文件对象
        BufferedReader bufferedReader = null;

        try {
            fileInputStream = new FileInputStream(file);
            bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
            String str = null;
            int counter = 0;
            while ((str = bufferedReader.readLine()) != null) {
                counter ++;
                if (counter <= 10 ) {
                    System.out.println(str);
                } else {
                    return;
                }
            }
            System.out.println("total:" + counter);

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
