package data.clean.demo;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
public class TestSort200W {
    public static void main(String[] args) {
        loadAndSort("E:\\work\\天数\\data\\136电脑\\MES_SL2_AL.csv");
    }

    /**
     * 读前两行
     * @param obsolutePath 文件
     * @return
     */
    public static Map<String,Object> loadAndSort(String obsolutePath) {
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
            Map<String,Object> map = new HashMap<>();
            System.out.println("start:" +simpleDateFormat.format(new Date()));
            while ((str = bufferedReader.readLine()) != null) {
                if (num == 0) {
                    num++;
                    continue;
                } else {
                    num++;
                    List<Object> list = new ArrayList<>();
                    String[] strings = str.split(",");
                    String strDate = strings[1];
                    String date = strDate.substring(2,strDate.length()-1);
                    Long l = simpleDateFormat.parse(date).getTime();
                    list.add(l);
                    if (strings.length > 2) {
                        list.add(strings[2]);
                    }

                    map.put(l.toString(),list);
                }
            }
            System.out.println("after load:" + simpleDateFormat.format(new Date()));
            Map<String, Object> sortMap = sortMapByKey(map);
            System.out.println("after sort:" + simpleDateFormat.format(new Date()));
            int coutn = 0;
            for (String key:sortMap.keySet()) {
                coutn++;
                System.out.println(simpleDateFormat.format(new Date(Long.parseLong(key))));
                if (coutn == 10) {
                    return null;
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
     * 使用 Map按key进行排序
     * @param map
     * @return
     */
    public static Map<String, Object> sortMapByKey(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return null;
        }

        Map<String, Object> sortMap = new TreeMap<String, Object>(new MapKeyComparator());

        sortMap.putAll(map);

        return sortMap;
    }

    public static class MapKeyComparator implements Comparator<String> {

        @Override
        public int compare(String str1, String str2) {
            long l1 = Long.parseLong(str1);
            long l2 = Long.parseLong(str2);
            if (l1 > l2) {
                return 1;
            }else if (l1 == l2) {
                return 0;
            } else {
                return -1;
            }
            //            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            try {
//                Date date1 = simpleDateFormat.parse(str1);
//                Date date2 = simpleDateFormat.parse(str2);
//                return date1.compareTo(date2);
//            } catch (ParseException e) {
//                e.printStackTrace();
//                throw new RuntimeException();
//            }

        }
    }


}
