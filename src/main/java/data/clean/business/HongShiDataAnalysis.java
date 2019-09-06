package data.clean.business;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class HongShiDataAnalysis {
    public static void main(String[] args) {
//        System.out.println("寻找开始结束:" + new Date());
//        Map<String,String> startEnd = findStartEnd("E:\\work\\天数\\数据清洗\\data\\136电脑");//四分钟
//        System.out.println("寻找开始结束:" + new Date());
        Map<String,String> startEnd = new HashMap<>();
        startEnd.put("start","2019-02-28 14:14:31");
        startEnd.put("end","2019-03-29 15:08:33");
//        System.out.println("截取:" + new Date());
//        dataSubAll("E:\\work\\天数\\数据清洗\\data\\136电脑",startEnd);//五分钟
//        System.out.println("截取:" + new Date());
        System.out.println("聚合:" + new Date());
        aggr("E:\\work\\天数\\数据清洗\\data\\136电脑",startEnd);//分钟粒度8分钟，小时粒度8分钟半
        System.out.println("聚合:" + new Date());
    }

    /**
     * 聚合
     * @param path
     */
    public static void aggr(String path,Map<String,String> startEnd) {
        File file = new File(path + "\\sub");
        if (file.exists()) {
            File[] files = file.listFiles();
            if (null == files || files.length == 0) {
                System.out.println("文件夹是空的!");
            } else {
                for (File file2 : files) {
                    if (file2.isDirectory()) {
                        continue;
                    } else {
                        if (file2.getAbsolutePath().matches(".+\\.csv")) {
                            try {
                                String start = startEnd.get("start");
                                String end = startEnd.get("end");
                                aggr(path + "\\aggr-min\\",file2.getName(),file2.getAbsolutePath(),start,end,60000);
                            } catch (Exception e) {
                                e.printStackTrace();
                                throw new RuntimeException(file2.getAbsolutePath() + "Exception");
                            }
                        }
                    }
                }

            }
        } else {
            System.out.println("文件不存在!");
        }
    }

    /**
     * 截取
     * @param path
     */
    public static void dataSubAll(String path, Map<String,String> startEnd) {
        File file = new File(path);
        if (file.exists()) {
            File[] files = file.listFiles();
            if (null == files || files.length == 0) {
                System.out.println("文件夹是空的!");
            } else {
                for (File file2 : files) {
                    if (file2.isDirectory()) {
                        continue;
                    } else {
                        if (file2.getAbsolutePath().matches(".+\\.csv")) {
                            try {
                                String start = startEnd.get("start");
                                String end = startEnd.get("end");
                                dataSub(path + "\\sub\\",file2.getName(),file2.getAbsolutePath(),start,end);
                            } catch (Exception e) {
                                e.printStackTrace();
                                throw new RuntimeException(file2.getAbsolutePath() + "Exception");
                            }
                        }
                    }
                }

            }
        } else {
            System.out.println("文件不存在!");
        }
    }

    /**
     * 找最大的开始时间和最小的结束时间
     * @param path
     */
    public static Map<String,String> findStartEnd(String path) {
        File file = new File(path);
        if (file.exists()) {
            File[] files = file.listFiles();
            if (null == files || files.length == 0) {
                System.out.println("文件夹是空的!");
                return null;
            } else {
                String maxDate = "";
                String minDateLast = "";
                String maxDateFile = "";
                String minDateLastFile = "";
                Map<String,String> res = new HashMap<>();
                int i = 0;
                for (File file2 : files) {

                    if (file2.isDirectory()) {
                        continue;
                    } else {
                        if (file2.getAbsolutePath().matches(".+\\.csv")) {
                            Map<String,String> map = readStartAndEnd(file2.getAbsolutePath());
                            //String dateStrLast = readLastDate(file2.getAbsolutePath());
//                            System.out.println(file2.getAbsolutePath());
//                            System.out.println("dateStrStart:" + map.get("start"));
//                            System.out.println("dateStrLast:" + map.get("end"));
                            if (i == 0) {
                                maxDate = map.get("start");
                                minDateLast =  map.get("end");
                                maxDateFile = file2.getAbsolutePath();
                                minDateLastFile = file2.getAbsolutePath();
                                i++;
                                continue;
                            }
                            if (maxDate.compareTo(map.get("start")) < 0) {
                                maxDateFile = file2.getAbsolutePath();
                                maxDate = map.get("start");
                            }
                            if (minDateLast.compareTo( map.get("end")) > 0) {
                                minDateLast =  map.get("end");
                                minDateLastFile = file2.getAbsolutePath();
                            }
                            //System.out.println("文件:" + file2.getAbsolutePath() + "处理完成");
                        }
                    }
                    i++;
                }
                System.out.println("maxDateStart:" + maxDate);
                System.out.println("maxDateStartFile:" + maxDateFile);
                System.out.println("minDateLast:" + minDateLast);
                System.out.println("minDateLastFile:" + minDateLastFile);
                res.put("start",maxDate);
                res.put("maxDateStartFile",maxDateFile);
                res.put("end",minDateLast);
                res.put("minDateLastFile",minDateLastFile);
                return res;
            }
        } else {
            System.out.println("文件不存在!");
            return null;
        }
    }

    /**
     * 读首条数据和最后一条数据
     * @param obsolutePath 文件
     * @return
     */
    public static Map<String,String> readStartAndEnd(String obsolutePath) {
        //BufferedReader是可以按行读取文件
        FileInputStream inputStream = null;
        BufferedReader bufferedReader = null;
        FileWriter fileWritter= null;
        try {
            inputStream = new FileInputStream(obsolutePath);
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String str = null;
            int num = 0;
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Map<String,String> map = new HashMap<>();
            while ((str = bufferedReader.readLine()) != null) {
                if (num == 0) {
                    num++;
                    continue;
                } else if (num == 1) {
                    num++;
                    String strDate = str.split(",")[1];
                    map.put("start",strDate.substring(2,strDate.length()-1));
                } else {
                    num++;
                    String strDate = str.split(",")[1];
                    map.put("end",strDate.substring(2,strDate.length()-1));
                }
            }
            return map;
        } catch (FileNotFoundException e) {
            log.error("file not found");
            throw new RuntimeException();
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw new RuntimeException();
        } catch (Exception e) {
            log.error("文件读取异常");
            throw new RuntimeException();
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
     * 数据切割
     * @param obsolutePath 文件
     * @return
     */
    public static void dataSub(String path,String saveName,String obsolutePath,String start,String end) throws IOException, ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long startl = simpleDateFormat.parse(start).getTime();
        long endl = simpleDateFormat.parse(end).getTime();
        //BufferedReader是可以按行读取文件
        FileInputStream inputStream = null;
        BufferedReader bufferedReader = null;
        File file = new File(path);
        String saveFileName = path + saveName;
        File file1 = new File(saveFileName);
        if(!file.exists()){
            file.mkdirs();
        }
        if(file1.exists()){
            file1.delete();
        }
        file1.createNewFile();
        FileWriter fileWritter = new FileWriter(saveFileName,true);
        try {
            inputStream = new FileInputStream(obsolutePath);
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String str = null;
            int num = 0;
            boolean isStart = false;
            while ((str = bufferedReader.readLine()) != null) {
                if (num == 0) {
                    num++;
                    fileWritter.write(str +  "\n");
                    continue;
                } else {
                    String strDate = str.split(",")[1];
                    String date =  strDate.substring(2,strDate.length()-1);
                    long datelong = simpleDateFormat.parse(date).getTime();
                    if (isStart) {
                        //超过结束时间
                        if (datelong > endl) {
                            break;
                        }
                        fileWritter.write(str +  "\n");
                    } else {
                        //发现开始数据
                        if (datelong >= startl) {
                            isStart = true;
                            fileWritter.write(str +  "\n");
                        }
                    }
                }
            }
        } catch (FileNotFoundException e) {
            log.error("file not found");
            throw new RuntimeException();
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw new RuntimeException();
        } catch (Exception e) {
            log.error("文件读取异常");
            throw new RuntimeException();
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
     * 聚合
     * @param obsolutePath 文件
     * @return
     */
    public static void aggr(String path,String saveName,String obsolutePath,String start,String end,int gran/*粒度*/) throws IOException, ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long startl = simpleDateFormat.parse(start).getTime();
        long startI = startl - startl % gran + gran;
        long endl = simpleDateFormat.parse(end).getTime();
        //BufferedReader是可以按行读取文件
        FileInputStream inputStream = null;
        BufferedReader bufferedReader = null;
        File file = new File(path);
        String saveFileName = path + saveName;
        File file1 = new File(saveFileName);
        if(!file.exists()){
            file.mkdirs();
        }
        if(file1.exists()){
            file1.delete();
        }
        file1.createNewFile();
        FileWriter fileWritter = new FileWriter(saveFileName,true);
        try {
            inputStream = new FileInputStream(obsolutePath);
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String str = null;
            int num = 0;
            double sum = 0;
            int count = 0;
            while ((str = bufferedReader.readLine()) != null) {
                if (num == 0) {
                    num++;
                    fileWritter.write(str +  "\n");
                    continue;
                } else {
                    String[] strings = str.split(",");
                    String strDate = strings[1];
                    String date =  strDate.substring(2,strDate.length()-1);
                    long datelong = simpleDateFormat.parse(date).getTime();
                    String val = null;
                    if (strings.length >= 3) {
                        val = strings[2];
                    }
                    if (startI > datelong) {
                        if (StringUtils.isNotBlank(val)){
                            sum += Double.parseDouble(val);
                            count++;
                        }
                    } else if (startI == datelong){
                        String res = "";
                        if (count > 0) {
                            res = formatDouble4(sum/count);
                        }
                        fileWritter.write(simpleDateFormat.format(new Date(startI - gran)) + "," + res +  "\n");
                        sum = 0;
                        count = 0;
                        startI += gran;
                        if (StringUtils.isNotBlank(val)){
                            sum += Double.parseDouble(val);
                            count++;
                        }
                    }
                }
            }
        } catch (FileNotFoundException e) {
            log.error("file not found");
            throw new RuntimeException();
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw new RuntimeException();
        } catch (Exception e) {
            log.error("文件读取异常");
            throw new RuntimeException();
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
}
