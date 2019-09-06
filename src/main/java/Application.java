import data.clean.core.DataFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.io.*;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
public class Application {
    /**
     * 匹配字符串格式
     */
    public static String[] dateTimePatterns = {"yyyy/MM/dd HH:mm:ss","yyyy/MM/dd HH:mm", "yyyy-MM-dd HH:mm:ss", "yyyy.MM.dd HH:mm:ss"};

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        String path = "E:\\work\\天数\\数据清洗\\漳平三期4月1日起新数据-张居宾\\";

        // 格式化日期数据，并按日期升序排序
        formatDirectory(path);

        //数据预处理 补值和移动平均
        //deal(path,10,30,true);

        //连接数据
        //joinDirectory(path,true);

        long diff  = System.currentTimeMillis() - start;
        System.out.println("执行耗时：" + diff + "ms");
        System.out.println("执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 格式化数据
     * 1.日期格式标准化
     * 2.按日期升序排序
     * 3.支持附属列为多列情况，但是标准列必须为第一列（以后优化，指定标准列）
     * 4.如果某行附属列数据全为空则舍弃该列
     * @param path 文件路径
     */
    public static void formatDirectory(String path) {

        String expandedName = "\\.txt";
        String bashPath = path;
        String sourcePath = bashPath + "source\\";
        String formatPath = bashPath + "formatSource\\";
        boolean existTitle = true;
        int lagCycle = 60;
        long start = System.currentTimeMillis();

        new DataFormat(expandedName,bashPath,sourcePath,formatPath,existTitle,lagCycle).formatDirectory();

        long diff  = System.currentTimeMillis() - start;
        log.info("FormatData执行耗时：" + diff + "ms");
        log.info("FormatData执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * joinDirectory
     * @param basePath
     */
    public static void joinDirectory(String basePath,boolean existHead) {

        String formatSourcePath = basePath + "formatSource\\";
        String movePath = basePath + "move\\";
        File file = new File(formatSourcePath);
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
                        if (file2.getAbsolutePath().matches(".+\\.txt")) {
                            join(file2.getName(), formatSourcePath + file2.getName(), basePath,movePath,"\t",existHead);
                            System.out.println("文件:" + file2.getAbsolutePath() + " join处理完成");
                        } else if (file2.getAbsolutePath().matches(".+\\.CSV")) {
                            join(file2.getName(), formatSourcePath + file2.getName(), basePath,movePath,",",existHead);
                            System.out.println("文件:" + file2.getAbsolutePath() + " join处理完成");
                        }
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }


    }


    /**
     * 连接文件
     * @param baseFilePath 文件
     * @return
     */
    public static void join(String joinFileName,String baseFilePath,String basePath,String movePath,String separate,boolean existHead) {
        //BufferedReader是可以按行读取文件
        FileInputStream inputStream = null;
        BufferedReader bufferedReader = null;
        FileWriter fileWritter= null;
        try {
            inputStream = new FileInputStream(baseFilePath + "");
            String pathJoin = basePath + "\\join\\";
            String joinFile = pathJoin + joinFileName;
            File file = new File(pathJoin);
            File file1 = new File(joinFile);
            if(!file.exists()){
                file.mkdirs();
            }
            if(file1.exists()){
                file1.delete();
            }
            file1.createNewFile();
            fileWritter = new FileWriter(joinFile,true);

            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

            List<List<Object>> listList = new ArrayList<>();
            List<Object> ts = new ArrayList<>();
            List<Object> res = new ArrayList<>();
            listList.add(ts);
            listList.add(res);

            String str = null;
            int counter = 0;
            while ((str = bufferedReader.readLine()) != null) {
                if (counter == 0) {
                    counter++;
                    if (existHead) {
                        continue;
                    }
                }
                String[] strings = StringUtils.splitPreserveAllTokens(str,separate);
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                if (strings == null || strings.length < 2) {
                    continue;
                }
                try{
                    Date value1 = simpleDateFormat.parse(strings[0]);
                    value1 = new Date(value1.getTime() - value1.getTime() % 60000);
                    ts.add(simpleDateFormat.format(value1));
                    String value2 = strings[1];
                    res.add(value2);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            System.out.println(ts.size());
            System.out.println(res.size());

            File moveFile = new File(movePath);

            if (moveFile.exists()) {
                File[] files = moveFile.listFiles();
                if (null == files || files.length == 0) {
                    System.out.println("文件夹是空的!");
                    return;
                } else {
                    int count = 1;
                    StringBuffer head = new StringBuffer("ts\tres\t");
                    for (File file2 : files) {
                        if (file2.isDirectory()) {
//                        System.out.println("文件夹:" + file2.getAbsolutePath());
//                        traverseFolder2(file2.getAbsolutePath());
                            continue;
                        } else {
                            if (file2.getAbsolutePath().matches(".+\\.txt")) {
                                String headSingle = match(file2.getAbsolutePath(),ts,listList,"\t",existHead);
                                if (count == files.length) {
                                    head.append(headSingle);
                                    break;
                                }
                                head.append(headSingle + "\t");
                                //System.out.println("文件:" + file2.getAbsolutePath() + "处理完成");
                            } else if (file2.getAbsolutePath().matches(".+\\.CSV")) {
                                String headSingle = match(file2.getAbsolutePath(),ts,listList,"\t",existHead);
                                if (count == files.length) {
                                    head.append(headSingle);
                                    break;
                                }
                                head.append(headSingle + "\t");
                                //System.out.println("文件:" + file2.getAbsolutePath() + "处理完成");
                            }

                        }
                    }
                    fileWritter.write(head.toString() + "\n");
                }
            } else {
                System.out.println("文件不存在!");
            }

            //取最短的数据长度为基准
            int minSize = 1000000000;
            for (int i = 0; i < listList.size(); i++) {
                int size = listList.get(i).size();
                if (size < minSize) {
                    minSize = size;
                }
            }

            for (int i = 0; i < minSize; i++) {
                String contact = "";
                for (int j = 0; j < listList.size(); j++) {
                    if (j == listList.size() - 1) {
                        contact += listList.get(j).get(i) + "";
                        break;
                    }
                    contact += listList.get(j).get(i)+ "\t";
                }
                fileWritter.write(contact + "\n");
            }

            System.out.println(listList.size());
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
     * 获取文件数据
     * @param filePath 文件
     * @return
     */
    public static void fill(String filePath,String fileName,String obsolutePath,int lag,boolean existHead) {
        //BufferedReader是可以按行读取文件
        FileInputStream inputStream = null;
        BufferedReader bufferedReader = null;
        FileWriter fileWritter= null;
        try {
            inputStream = new FileInputStream(obsolutePath);
            String pathFill = filePath + "\\fill\\";
            String pathFile = pathFill + fileName;
            File file = new File(pathFill);
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
            Date date = null;
            String str = null;
            boolean isAdd = true;
            int counter = 0;
            while ((str = bufferedReader.readLine()) != null) {

                if (counter == 0) {
                    counter++;
                    if (existHead) {
                        //写文件头
                        fileWritter.write(new String(str.getBytes("GB2312"),"UTF-8") + "\n");
                        continue;
                    }
                }

                String[] strings = StringUtils.splitPreserveAllTokens(str,",");
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                if (strings == null || strings.length < 3) {
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
                            dataStr += "\t";
                        }
                    }
                    Date value1 = DateUtils.parseDate(dateStr,dateTimePatterns);
                    value1 = new Date(value1.getTime() - value1.getTime() % 60000);
                    if (date == null ) {
                        date = value1;
                        String lagDate = lagDate(date,lag);
                        fileWritter.write(lagDate + "\t" + dataStr + "\n");
                        continue;
                    }
                    if (isAdd) {
                        date = new Date(date.getTime() + 60000);
                    }

                    if (date.compareTo(value1) == 0 ) {
                        String lagDate = lagDate(date,lag);
                        fileWritter.write(lagDate + "\t" + dataStr + "\n");
                        isAdd = true;
                    } else if (date.compareTo(value1) > 0 ) {
                        isAdd = false;
                    } else if (date.compareTo(value1) < 0 ) {
                        while (date.compareTo(value1) <= 0) {
                            if (date.compareTo(value1) == 0 ) {
                                String lagDate = lagDate(date,lag);
                                fileWritter.write(lagDate + "\t" + dataStr + "\n");
                                break;
                            }
                            String lagDate = lagDate(date,lag);
                            fileWritter.write(lagDate + "\t" + "" + "\n");
                            date = new Date(date.getTime() + 60000);
                        }
                        isAdd = true;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (FileNotFoundException e) {
            log.error("file not found");
        } catch (RuntimeException e) {
            e.printStackTrace();
        } catch (Exception e) {
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
     * match
     * 注意：认为ts的开始时间大于或者等于文件第一条数据的时间，并且时间跨度一样
     * @param obsolutePath 文件
     * @return
     */
    public static String match(String obsolutePath,List<Object> ts,List<List<Object>> listList,String separate,boolean existHead) {

        StringBuffer head = new StringBuffer();
        //BufferedReader是可以按行读取文件
        FileInputStream inputStream = null;
        BufferedReader bufferedReader = null;
        List<List<Object>> lists = new ArrayList<>();

        try {
            inputStream = new FileInputStream(obsolutePath);
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String str = null;
            int count = 0;
            int size = 0;
            while ((str = bufferedReader.readLine()) != null) {

                if (count >= ts.size()) {
                    break;
                }

                if (size == 0) {
                    size++;
                    if (existHead) {
                        String[] strings = StringUtils.splitPreserveAllTokens(str,separate);
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
                long source = simpleDateFormat.parse(ts.get(count).toString()).getTime();
                String[] strings = StringUtils.splitPreserveAllTokens(str,separate);
                long value1 =  simpleDateFormat.parse(strings[0]).getTime();

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
                        source = simpleDateFormat.parse(ts.get(count).toString()).getTime();
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
            log.error("file not found");
        } catch (RuntimeException e) {
            e.printStackTrace();
        } catch (Exception e) {
            log.error("文件读取异常");
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
        System.out.println(obsolutePath);
        return head.toString();
    }

    /**
     * 预处理
     * 1.按分钟为粒度空值补值
     * 2.按步长step向上移动取平均值，step以分钟为单位
     * @param path
     */
    public static void deal(String path,int step,int lag,boolean existHead) {

        File file = new File(path);
        if (file.exists()) {
            File[] files = file.listFiles();
            if (null == files || files.length == 0) {
                System.out.println("文件夹是空的!");
                return;
            } else {
                for (File file2 : files) {
                    if (file2.isDirectory()) {
//                        System.out.println("文件夹:" + file2.getAbsolutePath());
//                        traverseFolder2(file2.getAbsolutePath());
                        continue;
                    } else {
                        if (file2.getAbsolutePath().matches(".+\\.CSV")) {
                            fill(file2.getParent(),file2.getName(),file2.getAbsolutePath(),lag,existHead);
                            //moveAverage(file2.getParent(),file2.getName(),step,existHead);
                            moveMedian(file2.getParent(),file2.getName(),step,existHead);
                            System.out.println("文件:" + file2.getAbsolutePath() + "补值和移动平均处理完成");
                        }

                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
    }

    /**
     * 移动中位数
     * @param filePath
     * @param fileName
     */
    public static void moveMedian(String filePath,String fileName,int step,boolean existHead) {
        FileInputStream inputStream = null;
        BufferedReader bufferedReader = null;
        FileWriter fileWritter= null;
        try {

            String pathFill = filePath + "\\fill\\";
            String pathSave = filePath + "\\move\\";
            String pathSourceFile = pathFill + fileName;
            inputStream = new FileInputStream(pathSourceFile);
            String path = pathSave + fileName;
            File file = new File(pathSave);
            File file1 = new File(path);
            if(!file.exists()){
                file.mkdirs();
            }
            if(file1.exists()){
                file1.delete();
            }
            file1.createNewFile();
            fileWritter = new FileWriter(path,true);

            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String str;
            int count = 0;

            List<List<Double>> lists = new LinkedList();

            int counter = 0;
            while ((str = bufferedReader.readLine()) != null) {

                if (counter == 0) {
                    counter++;
                    if (existHead) {
                        //写文件头
                        fileWritter.write(str + "\n");
                        continue;
                    }
                }

                String[] strings = StringUtils.splitPreserveAllTokens(str,"\t");
                if (strings.length <= 0) {
                    continue;
                }
                count++;
                try{
                    String value1 = strings[0];
                    StringBuffer rowValue = new StringBuffer(value1);
                    for (int i = 1; i < strings.length; i++) {
                        String val = strings[i];
                        List<Double> values;
                        //首次初始化值
                        if (count == 1) {
                            values = new ArrayList<>();
                            lists.add(values);
                        } else {
                            //不是首次，获取缓存值
                            values = lists.get(i - 1);
                        }

                        //更新缓存值
                        if (StringUtils.isNotBlank(val)) {
                            double valDouble = Double.parseDouble(val);
                            values.add(valDouble);
                        } else {
                            values.add(null);
                        }

                        if (count > step) {
                            values.remove(0);
                        }
                        String median = median(values);
                        rowValue.append("\t" + median);
                    }
                    rowValue.append("\n");
                    fileWritter.write(rowValue.toString());


                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (FileNotFoundException e) {
            log.error("file not found");
        } catch (RuntimeException e) {
            e.printStackTrace();
        } catch (Exception e) {
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
     * 移动平均
     * @param filePath
     * @param fileName
     */
    public static void moveAverage(String filePath,String fileName,int step,boolean existHead) {
        FileInputStream inputStream = null;
        BufferedReader bufferedReader = null;
        FileWriter fileWritter= null;
        try {

            String pathFill = filePath + "\\fill\\";
            String pathSave = filePath + "\\move\\";
            String pathSourceFile = pathFill + fileName;
            inputStream = new FileInputStream(pathSourceFile);
            String path = pathSave + fileName;
            File file = new File(pathSave);
            File file1 = new File(path);
            if(!file.exists()){
                file.mkdirs();
            }
            if(file1.exists()){
                file1.delete();
            }
            file1.createNewFile();
            fileWritter = new FileWriter(path,true);

            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String str;
            int count = 0;

            double sum = 0;
            int nullCount = 0;
            List<Double> list = new LinkedList();

            List<Double> sums = new ArrayList<>();
            List<Integer> nullCounts = new ArrayList<>();
            List<List<Double>> lists = new LinkedList();

            int counter = 0;
            while ((str = bufferedReader.readLine()) != null) {

                if (counter == 0) {
                    counter++;
                    if (existHead) {
                        //写文件头
                        fileWritter.write(str + "\n");
                        continue;
                    }
                }

                String[] strings = StringUtils.splitPreserveAllTokens(str,"\t");
                if (strings.length <= 0) {
                    continue;
                }
                count++;
                try{
                    String value1 = strings[0];
                    StringBuffer rowValue = new StringBuffer(value1);
                    for (int i = 1; i < strings.length; i++) {
                        String val = strings[i];
                        List<Double> values;
                        Double sumD;
                        int nullC;
                        //首次初始化值
                        if (count == 1) {
                            values = new ArrayList<>();
                            lists.add(values);
                            sumD = 0d;
                            sums.add(sumD);
                            nullC = 0;
                            nullCounts.add(nullC);
                        } else {
                            //不是首次，获取缓存值
                            values = lists.get(i - 1);
                            sumD = sums.get(i -1);
                            nullC = nullCounts.get(i - 1);
                        }

                        //更新缓存值
                        if (StringUtils.isNotBlank(val)) {
                            double valDouble = Double.parseDouble(val);
                            values.add(valDouble);
                            sumD += valDouble;
                            sums.set(i - 1,sumD);
                        } else {
                            nullC++;
                            nullCounts.set(i - 1,nullC);
                            values.add(null);
                        }

                        if (count <= step) {
                            if ((int)Double.parseDouble(formatDouble4(sumD))== 0) {
                                rowValue.append("\t" + "");
                            } else {
                                rowValue.append("\t" + formatDouble4(sumD * 1.0/(count - nullC)));
                            }
                        } else {
                            Double remove = values.remove(0);
                            if (null != remove) {
                                sumD -= remove;
                                sums.set(i - 1,sumD);
                            } else {
                                nullC--;
                            }

                            if ((int)Double.parseDouble(formatDouble4(sumD))== 0) {
                                rowValue.append("\t" + "");
                            } else {
                                rowValue.append("\t" + formatDouble4(sumD * 1.0/(step - nullC)));
                            }
                        }
                    }
                    rowValue.append("\n");
                    fileWritter.write(rowValue.toString());

                    //                    String value2 = null;
//                    if (strings.length > 1) {
//                        value2 = strings[1];
//                    }
//                    if (StringUtils.isNotBlank(value2)) {
//                        double val = Double.parseDouble(value2);
//                        list.add(val);
//                        sum += val;
//                    } else {
//                        nullCount++;
//                        list.add(null);
//                    }
//                    if (count <= step) {
//                        if ((int)Double.parseDouble(formatDouble4(sum))== 0) {
//                            fileWritter.write(value1 + "\t" + "" + "\n");
//                        } else {
//                            fileWritter.write(value1 + "\t" + formatDouble4(sum * 1.0/(count - nullCount)) + "\n");
//                        }
//                    } else {
//                        Double remove = list.remove(0);
//                        if (null != remove) {
//                            sum -= remove;
//                        } else {
//                            nullCount--;
//                        }
//
//                        if ((int)Double.parseDouble(formatDouble4(sum))== 0) {
//                            fileWritter.write(value1 + "\t" + "" + "\n");
//                        } else {
//                            fileWritter.write(value1 + "\t" + formatDouble4(sum * 1.0/(step - nullCount)) + "\n");
//                        }
//                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (FileNotFoundException e) {
            log.error("file not found");
        } catch (RuntimeException e) {
            e.printStackTrace();
        } catch (Exception e) {
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
     * 保留小数
     * @param d
     * @return
     */
    public static String formatDouble4(double d) {
        DecimalFormat df = new DecimalFormat("#.000000");
        return df.format(d);
    }

    /**
     * 滞后日期
     * @param date
     * @param lag
     * @return
     */
    public static String lagDate(String date, int lag) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date keyDate = DateUtils.parseDate(date,dateTimePatterns);
        keyDate = new Date(keyDate.getTime() + lag * 60 * 1000);
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

    /**
     * 中位数
     * @param list
     * @return
     */
    public static String median(List<Double> list){
        if (null == list || list.size() == 0) {
            return "";
        }
        List<Double> noNullList = new ArrayList();
        for (int i = 0; i < list.size(); i++) {
            if (null != list.get(i)) {
                noNullList.add(list.get(i));
            }
        }

        if (noNullList.size() == 0) {
            return "";
        }

        // list升序排序
        Collections.sort(noNullList);

        // 生成中位数
        Double j;
        if (noNullList.size() % 2 == 0) {
            j = (noNullList.get(noNullList.size() / 2 - 1) + noNullList.get(noNullList.size() / 2)) / 2;

        } else {
            j = noNullList.get(noNullList.size() / 2);
        }

        return j.toString();
    }
}