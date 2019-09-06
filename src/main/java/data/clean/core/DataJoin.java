package data.clean.core;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 数据连接
 */
public class DataJoin {
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
    //特征数据路径
    public String charDataPath;
    //是否存在标题
    public boolean existTitle;
    //源文件编码
    public String charset;

    public DataJoin(String expandedName, String separatorChars, String sourcePath, String targetPath, String charDataPath, boolean existTitle, String charset) {
        this.expandedName = expandedName;
        this.separatorChars = separatorChars;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.charDataPath = charDataPath;
        this.existTitle = existTitle;
        this.charset = charset;
    }

    public static void main(String[] args) {

        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\source\\";
        String sourcePath = bashPath;
        String charDataPath = bashPath + "特征数据-位号滞后55\\";
        String targetPath = bashPath + "join-位号滞后55\\";
        DataJoin dataJoin = new DataJoin(expandedName,
                ",",
                sourcePath,targetPath,charDataPath,
                true,"GBK");

        long start = System.currentTimeMillis();

        dataJoin.dataJoin();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据滞后处理执行耗时：" + diff + "ms");
        System.out.println("数据滞后处理执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 数据滞后处理
     */
    public void dataJoin() {
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
                            dataJoin(file2);
                            System.out.println("文件:" + file2.getAbsolutePath() + " dataJoin处理完成");
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
    public void dataJoin(File file) {
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

            List<List<Object>> listList = new ArrayList<>();
            List<Object> ts = new ArrayList<>();
            List<Object> res = new ArrayList<>();
            listList.add(ts);
            listList.add(res);

            StringBuffer title = new StringBuffer();
            String str = null;
            int counter = 0;
            while ((str = bufferedReader.readLine()) != null) {
                if (counter == 0) {
                    counter++;
                    if (existTitle) {
                        title.append(str);
                        continue;
                    }
                }
                String[] strings = StringUtils.splitPreserveAllTokens(str,separatorChars);

                if (strings == null || strings.length < 2) {
                    continue;
                }
                try{
                    Date date = DateUtils.parseDate(strings[0],dateTimePatterns);
                    date = new Date(date.getTime() - date.getTime() % 60000);
                    ts.add(DateFormatUtils.format(date,formatDate));
                    StringBuffer value = new StringBuffer();
                    for (int i = 1; i < strings.length; i++) {
                        if (i == 1) {
                            value.append(strings[i]);
                        } else {
                            value.append(separatorChars + strings[i]);
                        }
                    }

                    res.add(value.toString());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            System.out.println(ts.size());
            System.out.println(res.size());

            File charDataFile = new File(charDataPath);

            if (charDataFile.exists()) {
                File[] files = charDataFile.listFiles();
                if (null == files || files.length == 0) {
                    System.out.println("文件夹是空的!");
                    return;
                } else {
                    int count = 1;
                    for (File file2 : files) {
                        if (file2.isDirectory()) {
                            continue;
                        } else {
                            String headSingle = match(file2,ts,listList);
                            title.append(separatorChars + headSingle);
                            System.out.println("文件:" + file2.getAbsolutePath() + "join处理完成");
                        }
                    }
                    fileWriter.write(title.toString() + "\n");
                }
            } else {
                System.out.println("文件不存在!");
            }

            //取最短的数据长度为基准
            int minSize = 1000000000;
//            for (int i = 0; i < listList.size(); i++) {
//                int size = listList.get(i).size();
//                if (size < minSize) {
//                    minSize = size;
//                }
//            }

            for (int i = 0; i < res.size(); i++) {
                String contact = "";
                for (int j = 0; j < listList.size(); j++) {
                    if (j == listList.size() - 1) {
                        contact += listList.get(j).get(i) + "";
                        break;
                    }
                    contact += listList.get(j).get(i)+ separatorChars;
                }
                fileWriter.write(contact + "\n");
            }

            System.out.println(listList.size());

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
     * match
     * 注意：认为ts的开始时间大于或者等于文件第一条数据的时间，并且时间跨度一样
     * 已优化：ts可以小于第一条数据，但时间跨度一样
     * @param file 文件
     * @return
     */
    public String match(File file,List<Object> ts,List<List<Object>> listList) {

        StringBuffer head = new StringBuffer();
        //BufferedReader是可以按行读取文件
        FileInputStream inputStream = null;
        BufferedReader bufferedReader = null;
        List<List<Object>> lists = new ArrayList<>();

        try {
            inputStream = new FileInputStream(file);
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String str = null;
            int count = 0;
            int size = 0;
            while ((str = bufferedReader.readLine()) != null) {

                if (count >= ts.size()) {
                    break;
                }

                if (size == 0) {
                    size++;
                    if (existTitle) {
                        String[] strings = StringUtils.splitPreserveAllTokens(str,separatorChars);
                        for (int i = 1; i < strings.length; i++) {
                            if (i == strings.length - 1) {
                                head.append(strings[i]);
                                break;
                            }
                            head.append(strings[i] + ",");
                        }
                        continue;
                    }
                }
                long source = DateUtils.parseDate(ts.get(count).toString(),dateTimePatterns).getTime();
                String[] strings = StringUtils.splitPreserveAllTokens(str,separatorChars);
                long value1 = DateUtils.parseDate(strings[0],dateTimePatterns).getTime();

                boolean isAdd = false;

                if (source > value1) {
                    continue;
                } else if (source == value1) {
                    isAdd = true;
                    for (int i = 1; i < strings.length; i++) {
                        String val = strings[i];
                        List<Object> data;
                        //首次初始化值
                        if (count == 0) {
                            data = new ArrayList<>(ts.size());
                            lists.add(data);
                        } else {
                            //不是首次，获取缓存值
                            data = lists.get(i - 1);
                        }
                        data.add(count,val);
                    }
                } else if (source < value1) {
                    while (source <= value1) {
                        for (int i = 1; i < strings.length; i++) {
                            String val = strings[i];
                            List<Object> data;
                            //首次初始化值
                            if (count == 0) {
                                data = new ArrayList<>(ts.size());
                                lists.add(data);
                            } else {
                                //不是首次，获取缓存值
                                data = lists.get(i - 1);
                            }

                            if (source == value1) {
                                data.add(count,val);
                            } else {
                                data.add(count,"");
                            }
                        }
                        count++;
                        if (count >= ts.size()) {
                            break;
                        }
                        source =  DateUtils.parseDate(ts.get(count).toString(),dateTimePatterns).getTime();
                    }
                }

                if (isAdd) {
                    count++;
                }
                if (count >= ts.size()) {
                    break;
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("file not found");
        } catch (RuntimeException e) {
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("文件读取异常");
        } finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        listList.addAll(lists);
        System.out.println(head.toString());
        return head.toString();
    }

}