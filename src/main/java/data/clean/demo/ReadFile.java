package data.clean.demo;

import java.io.*;
import java.util.HashSet;

public class ReadFile {
    public static void main(String[] args) {
        File file = new File("C:\\Users\\dell\\AppData\\Roaming\\ZhiXin\\李佳鹤\\风煤料预测数据集.csv");
        readFile(file);
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
            while ((str = bufferedReader.readLine()) != null) {
                String[] strings = str.split(",");
                System.out.println(strings.length);
                HashSet set = new HashSet();

                for (int i = 0; i < strings.length; i++) {
                    set.add(strings[i]);
                    if ((i + 1) != set.size()) {
                        System.out.println(i);
                        break;
                    }
                }
                break;
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
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
