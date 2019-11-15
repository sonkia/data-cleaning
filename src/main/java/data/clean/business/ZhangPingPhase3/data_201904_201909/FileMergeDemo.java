package data.clean.business.ZhangPingPhase3.data_201904_201909;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class FileMergeDemo {
    public static void main(String[] args) {
        //文件流
        FileInputStream fileInputStream = null;
        //读文件对象
        BufferedReader bufferedReader = null;
        //写文件对象
        FileWriter fileWriter = null;
        try {
            fileInputStream = new FileInputStream("E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号数据\\QIAN130_1.csv");

            File dealFile = new File("E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号数据\\QIAN130.csv");

            fileWriter = new FileWriter(dealFile,true);
            bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));

            String str = null;
            while ((str = bufferedReader.readLine()) != null) {
                fileWriter.write(str + "\n");
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
//                if (null != fileWriter) {
//                    fileWriter.close();
//                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            fileInputStream = new FileInputStream("E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号数据\\QIAN130_2.csv");

            bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));

            String str = null;
            int count  = 0;
            while ((str = bufferedReader.readLine()) != null) {
                count++;
                if (count <= 2) {
                    continue;
                }
                fileWriter.write(str + "\n");
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
