package data.clean.core;

import data.clean.common.enums.AggrType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DataAggrFunction {


    /**
     * 聚合
     * @param list 数据
     * @param aggrType 聚合类型
     * @return
     */
    public static String aggr(List<Double> list,AggrType aggrType) {
        if (list == null || list.size() == 0 || aggrType == null) {
            return "";
        }
        switch (aggrType) {
            case MEDIAN:
                return median(list);
            case MEAN:
                return mean(list);
            default:
                return "";
        }
    }

    /**
     * 平均值
     * @param list
     * @return
     */
    public static String mean(List<Double> list) {
        if (null == list || list.size() == 0) {
            return "";
        }
        int noNullCount = 0;
        double sum = 0;
        for (int i = 0; i < list.size(); i++) {
            if (null != list.get(i)) {
                noNullCount++;
                sum += list.get(i);
            }
        }
        if (noNullCount == 0) {
            return "";
        }

        Double mean = sum / noNullCount;


        return mean.toString();
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
        Double median;
        if (noNullList.size() % 2 == 0) {
            median = (noNullList.get(noNullList.size() / 2 - 1) + noNullList.get(noNullList.size() / 2)) / 2;

        } else {
            median = noNullList.get(noNullList.size() / 2);
        }

        return median.toString();
    }
}
