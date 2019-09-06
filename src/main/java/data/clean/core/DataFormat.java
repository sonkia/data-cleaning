package data.clean.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
public class DataFormat {

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
    //滞后周期（单位分钟） 1：加一分钟  -2：减两分钟
    public int lagCycle;

    public DataFormat(String expandedName, String bashPath, String sourcePath, String targetPath, boolean existTitle, int lagCycle) {
        this.expandedName = expandedName;
        this.bashPath = bashPath;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.existTitle = existTitle;
        this.lagCycle = lagCycle;
    }

    public static void main(String[] args) {
        String expandedName = ".+\\.txt";
        String bashPath = "E:\\work\\天数\\数据清洗\\游离钙分析-move-median\\";
        String sourcePath = bashPath + "source\\";
        String targetPath = bashPath + "formatSource\\";
        boolean existTitle = true;
        int lagCycle = 60;
        long start = System.currentTimeMillis();

        new DataFormat(expandedName,bashPath,sourcePath,targetPath,existTitle,lagCycle).formatDirectory();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("FormatData执行耗时：" + diff + "ms");
        System.out.println("FormatData执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 格式化数据
     * 1.日期格式标准化
     * 2.按日期升序排序
     * 3.支持附属列为多列情况，但是标准列必须为第一列（以后优化，指定标准列）
     * 4.如果某行附属列数据全为空则舍弃该列
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
            List<List<Object>> listList = new ArrayList<>();
            List<Object> ts = new ArrayList<>();
            List<Object> res = new ArrayList<>();
            listList.add(ts);
            listList.add(res);
            Map<String,Object> map = new LinkedHashMap<>();
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
                String[] strings = StringUtils.splitPreserveAllTokens(str,"\t");
                if (strings == null || strings.length < 2) {
                    continue;
                }
                try{
                    Date value1 = DateUtils.parseDate(strings[0],dateTimePatterns);
                    //按分钟取整
                    value1 = new Date(value1.getTime() - value1.getTime() % 60000);
                    ts.add(simpleDateFormat.format(value1));
                    String value2 = "";
                    for (int i = 1; i < strings.length; i++) {
                        if (i == strings.length - 1) {
                            value2 += strings[i];
                        } else {
                            value2 += strings[i];
                            value2 += "\t";
                        }
                    }
                    if (StringUtils.isBlank(value2)) {
                        continue;
                    }
                    res.add(value2);
                    map.put(simpleDateFormat.format(value1),value2);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            Map<String, Object> sortMap = sortMapByKey(map);
            for(String key:sortMap.keySet()){
                String lagDate = lagDate(key);
                fileWritter.write(lagDate + "\t" + sortMap.get(key) + "\n");
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

    /**
     * 使用 Map按key进行排序
     * @param map
     * @return
     */
    public static Map<String, Object> sortMapByKey(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return null;
        }

        Map<String, Object> sortMap = new TreeMap<String, Object>(
                new MapKeyComparator());

        sortMap.putAll(map);

        return sortMap;
    }

    /**
     * 比较器
     */
    public static class MapKeyComparator implements Comparator<String> {
        @Override
        public int compare(String str1, String str2) {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                Date date1 = simpleDateFormat.parse(str1);
                Date date2 = simpleDateFormat.parse(str2);
                return date1.compareTo(date2);
            } catch (ParseException e) {
                e.printStackTrace();
                throw new RuntimeException();
            }
        }
    }

    /**
     * 滞后日期
     * @param date
     * @return
     */
    public String lagDate(String date) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date keyDate = DateUtils.parseDate(date,dateTimePatterns);
        keyDate = new Date(keyDate.getTime() + lagCycle * 60 * 1000);
        return simpleDateFormat.format(keyDate);
    }

    /**
     * 滞后日期
     * @param date
     * @param lag
     * @return
     */
    public static String lagDate(Date date, int lag){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date keyDate = new Date(date.getTime() + lag * 60 * 1000);
        return simpleDateFormat.format(keyDate);
    }

}