package data.clean.demo;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.*;

public class Test {

    public static void main(String[] args) throws ParseException {

        String[] strings = "QAR_SETDATE,F-CAO,三线1#生料磨_0.08细度,三线1#生料磨_0.20细度,三线2#生料磨_0.08细度,三线2#生料磨_0.20细度,三线1#生料磨_KH,三线1#生料磨_N,三线1#生料磨_P,三线1#生料磨_MGO,三线1#生料磨_SIO2,三线1#生料磨_AL2O3,三线1#生料磨_FE2O3,三线1#生料磨_CAO,三线1#生料磨_细度,三线1#生料磨_水分,三线1#生料磨_发热量,三线2#生料磨_细度,三线2#生料磨_水分,三线2#生料磨_发热量,窑电流01,窑转速01,窑电流02,窑转速02,分解炉出口温度,三次风温度,高温风机1电流,高温风机2电流,尾排风机电流,421FN03VF窑尾排风机频率反馈,高温风机1反馈,高温风机2反馈,窑头罩负压,分解炉出口压力,C6B锥部压力,C6A锥部压力,C5B锥部压力,C5A锥部压力,C5A下料管温度,C6B下料管温度,C6A下料管温度,C5B下料管温度,头煤流量反馈,尾煤流量反馈2,尾煤流量反馈,喂料量,烟囱出口氧含量,电收尘入口温度,煤磨入口温度01,煤磨入口温度02,二次风温,AI761RB03出口压力,AI761RB02出口压力,AI761RB01出口压力,AI761RB04出口压力,罗茨风机电流反馈01,罗茨风机电流反馈02,罗茨风机电流反馈03,罗茨风机电流反馈04,一段篦速,二段篦速,471FN17窑头排风机电流,471FN17窑头排风机频率,篦冷机速度反馈11,篦冷机速度反馈12,篦冷机速度反馈13,篦冷机速度反馈14,篦冷机速度反馈6,篦冷机速度反馈7,篦冷机速度反馈8,篦冷机速度反馈9,篦冷机速度反馈10,篦冷机电流反馈11,篦冷机电流反馈12,篦冷机电流反馈13,篦冷机电流反馈14,篦冷机电流反馈6,篦冷机电流反馈7,篦冷机电流反馈8,篦冷机电流反馈9,篦冷机电流反馈10,篦冷机电流反馈1,篦冷机电流反馈2,篦冷机电流反馈3,篦冷机电流反馈4,篦冷机速度反馈1,篦冷机速度反馈2,篦冷机速度反馈3,篦冷机速度反馈4".split(",",110);
        Set set = new HashSet();
        for (int s = 0; s < strings.length; s++) {
            set.add(strings[s]);
            if ((s+1) > set.size()) {
                System.out.println(s);
                throw new RuntimeException("chongfu");
            }
        }
        if (set.size() != strings.length) {
            System.out.println("chonfu");
        }
        System.out.println(set.size());
        System.out.println(strings.length);
    }

    public static Number formatDouble4(String d) throws ParseException {
        DecimalFormat df = new DecimalFormat("#.000000");
        return df.parse(d);
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
