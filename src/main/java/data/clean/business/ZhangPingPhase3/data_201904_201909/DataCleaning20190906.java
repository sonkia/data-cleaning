package data.clean.business.ZhangPingPhase3.data_201904_201909;

import data.clean.common.enums.AggrType;
import data.clean.common.enums.IntervalType;
import data.clean.core.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DataCleaning20190906 {


    public static void main(String[] args) {

        //step00(); //文件合并
        //step01();//秒级数据按平均值聚合成分钟级数据
        //step02();//文件数据整齐，多个文件合成一个文件
        //step03();//篦冷机数据拆分
        //step04();//预热器、分解炉数据拆分
        //step05();//回转窑数据拆分
        //step06();//分解炉及窑头数据拆分
        //step07();//分解炉及窑头数据按列平均聚合
        //step08();//分解炉及窑头和回转窑合并--step05和step07数据合并
        //step09();//篦冷机数据聚合
        //step10();//预热器、分解炉数据聚合
        //step11();//回转窑数据聚合
        //step12();//煤粉数据不同位置数据按行做平局聚合
        //step13();//原料数据滞后处理（加60分钟）
        //step14();//数据连接位号数据按小时采样
        step15();//数据连接
    }

    /**
     * 秒级数据按平均值聚合成分钟级数据
     */
    public static void step01() {
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号数据\\";
        String sourcePath = bashPath;
        String targetPath = bashPath + "step-01-位号数据：秒级数据聚合成分钟级数据\\";
        DataAggregationSecond2Minute dataAggregation = new DataAggregationSecond2Minute(expandedName,
                ",",
                sourcePath,targetPath,
                true,60,AggrType.MEAN,2,"GBK");

        long start = System.currentTimeMillis();

        dataAggregation.dataAggregation();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据聚合执行耗时：" + diff + "ms");
        System.out.println("数据聚合执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 文件数据整齐，多个文件合成一个文件
     */
    public static void step02() {
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号数据\\";
        String sourcePath = bashPath + "step-01-位号数据：秒级数据聚合成分钟级数据\\";
        String targetPath = bashPath + "step-02-位号数据文件合一\\";
        ColumnMerge columnMerge = new ColumnMerge(expandedName,
                ",",
                sourcePath,targetPath,
                true,"UTF-8");

        long start = System.currentTimeMillis();

        columnMerge.columnMerge();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据聚合执行耗时：" + diff + "ms");
        System.out.println("数据聚合执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 篦冷机数据拆分
     */
    public static void step03() {
        String columnsStr = "COMVFD471FN13_3.PV\n" +
                "COMVFD471FN14_3.PV\n" +
                "DP471FN01VF_AI10\n" +
                "DP471FN02VF_AI10\n" +
                "DP471FN03VF_AI10\n" +
                "DP471FN04VF_AI10\n" +
                "DP471FN05VF_AI10\n" +
                "COMVFD471FN06_3.PV\n" +
                "COMVFD471FN07_3.PV\n" +
                "COMVFD471FN08_3.PV\n" +
                "COMVFD471FN09_3.PV\n" +
                "COMVFD471FN10_3.PV\n" +
                "COMVFD471FN11_3.PV\n" +
                "COMVFD471FN12_3.PV\n" +
                "COMVFD471FN13_2.PV\n" +
                "COMVFD471FN14_2.PV\n" +
                "DP471FN01VF_AI05\n" +
                "DP471FN02VF_AI05\n" +
                "DP471FN03VF_AI05\n" +
                "DP471FN04VF_AI05\n" +
                "DP471FN05VF_AI05\n" +
                "COMVFD471FN06_2.PV\n" +
                "COMVFD471FN07_2.PV\n" +
                "COMVFD471FN08_2.PV\n" +
                "COMVFD471FN09_2.PV\n" +
                "COMVFD471FN10_2.PV\n" +
                "COMVFD471FN11_2.PV\n" +
                "COMVFD471FN12_2.PV";
        String[] columns = columnsStr.split("\\n");
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号数据\\";
        String sourcePath = bashPath + "step-02-位号数据文件合一\\bitMerge3.csv";
        File file = new File(bashPath + "step-03-篦冷机数据拆分");
        if (!file.exists()) {
            file.mkdirs();
        }
        String targetPath = bashPath + "step-03-篦冷机数据拆分\\篦冷机.csv";
        ColumnSplit columnSplit = new ColumnSplit(expandedName,
                ",",
                sourcePath,targetPath,
                true,"UTF-8",columns);

        long start = System.currentTimeMillis();

        columnSplit.columnSplit();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据聚合执行耗时：" + diff + "ms");
        System.out.println("数据聚合执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 预热器、分解炉数据拆分
     */
    public static void step04() {
        String columnsStr = "PA441PR01P02\n" +
                "PA441PR01T02\n" +
                "PA_471KH01T01\n" +
                "PA44101T02\n" +
                "PA441C5AT02\n" +
                "PA441C5BT02\n" +
                "PA441C6AT02\n" +
                "PA441C6BT02\n" +
                "PA441C5AP02\n" +
                "PA441C5BP02\n" +
                "PA441C6BP02\n" +
                "AI421FN01VF_PL\n" +
                "AI421FN02VF_PL\n" +
                "AI421FN02VF_II\n" +
                "DP421FN01VF_AI10\n" +
                "AI421FN03VF_PL";
        String[] columns = columnsStr.split("\\n");
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号数据\\";
        String sourcePath = bashPath + "step-02-位号数据文件合一\\bitMerge3.csv";
        File file = new File(bashPath + "step-04-预热器-分解炉数据拆分");
        if (!file.exists()) {
            file.mkdirs();
        }
        String targetPath = bashPath + "step-04-预热器-分解炉数据拆分\\预热器-分解炉.csv";
        ColumnSplit columnSplit = new ColumnSplit(expandedName,
                ",",
                sourcePath,targetPath,
                true,"UTF-8",columns);

        long start = System.currentTimeMillis();

        columnSplit.columnSplit();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据聚合执行耗时：" + diff + "ms");
        System.out.println("数据聚合执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 回转窑数据拆分
     */
    public static void step05() {
        String columnsStr = "DP431RO01F01_AI03Z\n" +
                "COMVFD761RB01_3\n" +
                "COMVFD761RB02_3\n" +
                "COMVFD761RB03_3\n" +
                "COMVFD761RB04_3\n" +
                "AI461KL01MT01AII\n" +
                "AI461KL01MT01BII\n" +
                "AI461KL01MT01ASI\n" +
                "AI461KL01MT01BSI\n" +
                "PA_471KH01P01";
        String[] columns = columnsStr.split("\\n");
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号数据\\";
        String sourcePath = bashPath + "step-02-位号数据文件合一\\bitMerge3.csv";
        File file = new File(bashPath + "step-05-回转窑数据拆分");
        if (!file.exists()) {
            file.mkdirs();
        }
        String targetPath = bashPath + "step-05-回转窑数据拆分\\回转窑.csv";
        ColumnSplit columnSplit = new ColumnSplit(expandedName,
                ",",
                sourcePath,targetPath,
                true,"UTF-8",columns);

        long start = System.currentTimeMillis();

        columnSplit.columnSplit();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据聚合执行耗时：" + diff + "ms");
        System.out.println("数据聚合执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 分解炉及窑头数据拆分
     */
    public static void step06() {
        String columnsStr = "DP761RS01_AI03\n" +
                "DP761RS02_AI03\n" +
                "DP761RS03_AI03";
        String[] columns = columnsStr.split("\\n");
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号数据\\";
        String sourcePath = bashPath + "step-02-位号数据文件合一\\bitMerge3.csv";
        File file = new File(bashPath + "step-06-分解炉及窑头数据拆分");
        if (!file.exists()) {
            file.mkdirs();
        }
        String targetPath = bashPath + "step-06-分解炉及窑头数据拆分\\分解炉及窑头.csv";
        ColumnSplit columnSplit = new ColumnSplit(expandedName,
                ",",
                sourcePath,targetPath,
                true,"UTF-8",columns);

        long start = System.currentTimeMillis();

        columnSplit.columnSplit();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据聚合执行耗时：" + diff + "ms");
        System.out.println("数据聚合执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 分解炉及窑头数据按列平均聚合
     */
    public static void step07(){
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号数据\\";
        String sourcePath = bashPath + "step-06-分解炉及窑头数据拆分\\";
        String targetPath = bashPath + "step-07-分解炉及窑头数据按列平均聚合\\";
        List<Integer> columnIndexes = new ArrayList<>();
        columnIndexes.add(1);
        columnIndexes.add(2);
        columnIndexes.add(3);
        DataColumnAggregation dataAggregation = new DataColumnAggregation(expandedName,
                ",",
                sourcePath,targetPath,
                true,columnIndexes,AggrType.MEAN,"UTF-8");

        long start = System.currentTimeMillis();

        dataAggregation.dataAggregation();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据聚合执行耗时：" + diff + "ms");
        System.out.println("数据聚合执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 分解炉及窑头和回转窑合并
     */
    public static void step08(){
        String expandedName = ".+\\.csv";
        String file1 = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号数据\\step-05-回转窑数据拆分\\回转窑.csv";
        String file2 = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号数据\\step-07-分解炉及窑头数据按列平均聚合\\分解炉及窑头.csv";
        String file = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号数据\\step-08-分解炉及窑头和回转窑合并\\";
        File filePath = new File(file);
        if (!filePath.exists()) {
            filePath.mkdirs();
        }
        String mergeFile = file + "回转窑.csv";
        TwoFileColumnMerge twoFileColumnMerge = new TwoFileColumnMerge(expandedName,
                ",",
                file1,file2,mergeFile,
                true,"UTF-8");

        long start = System.currentTimeMillis();

        twoFileColumnMerge.fileMerge();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据聚合执行耗时：" + diff + "ms");
        System.out.println("数据聚合执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 篦冷机数据聚合
     * 取前30分钟数据平均值聚合
     */
    public static void step09() {
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号数据\\";
        String sourcePath = bashPath + "step-03-篦冷机数据拆分\\";
        String targetPath = bashPath + "step-09-篦冷机数据聚合（前30分钟平均值）\\";
        DataAggregation dataAggregation = new DataAggregation(expandedName,
                ",",
                sourcePath,targetPath,
                true,30,AggrType.MEAN,1,"UTF-8");

        long start = System.currentTimeMillis();

        dataAggregation.dataAggregation();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据聚合执行耗时：" + diff + "ms");
        System.out.println("数据聚合执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 预热器、分解炉数据聚合
     * 取前70分钟的70条数据，对这70条数据取前20条数据做平均值聚合
     */
    public static void step10() {
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号数据\\";
        String sourcePath = bashPath + "step-04-预热器-分解炉数据拆分\\";
        String targetPath = bashPath + "step-10-预热器-分解炉数据拆分（取前70分钟的前20条数据平均值）\\";
        DataAggregationEnhance dataAggregation = new DataAggregationEnhance(expandedName,
                ",",
                sourcePath,targetPath,
                true,70,20,AggrType.MEAN,1,"UTF-8");

        long start = System.currentTimeMillis();

        dataAggregation.dataAggregation();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据聚合执行耗时：" + diff + "ms");
        System.out.println("数据聚合执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 回转窑数据聚合
     * 取前60分钟的60条数据，对这60条数据取前30条数据做平均值聚合
     */
    public static void step11() {
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号数据\\";
        String sourcePath = bashPath + "step-08-分解炉及窑头和回转窑合并\\";
        String targetPath = bashPath + "step-11-回转窑（取前60分钟的前30条数据平均值）\\";
        DataAggregationEnhance dataAggregation = new DataAggregationEnhance(expandedName,
                ",",
                sourcePath,targetPath,
                true,60,30,AggrType.MEAN,1,"UTF-8");

        long start = System.currentTimeMillis();

        dataAggregation.dataAggregation();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据聚合执行耗时：" + diff + "ms");
        System.out.println("数据聚合执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 煤粉数据不同位置数据按行做平局聚合
     *
     */
    public static void step12() {
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\原料数据\\";
        String sourcePath = bashPath + "煤粉\\";
        String targetPath = bashPath;
        RowMerge rowMerge = new RowMerge(expandedName,
                ",",
                sourcePath,targetPath,
                2,2,AggrType.MEAN,true,"GB2312");

        long start = System.currentTimeMillis();

        rowMerge.rowMerge();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据聚合执行耗时：" + diff + "ms");
        System.out.println("数据聚合执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 原料数据滞后处理（加60分钟）
     *
     */
    public static void step13() {
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\原料数据\\";
        String sourcePath = bashPath;
        String targetPath = bashPath + "step-13-原料数据滞后处理（加60分钟）\\";
        DataLagProcessing dataLagProcessing = new DataLagProcessing(expandedName,
                ",",
                sourcePath,targetPath,
                true,60,"UTF-8");

        long start = System.currentTimeMillis();

        dataLagProcessing.lagProcessing();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据滞后处理执行耗时：" + diff + "ms");
        System.out.println("数据滞后处理执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 位号数据按小时采样
     */
    public static void step14() {
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\source\\";
        String sourcePath = bashPath + "特征数据\\";
        String targetPath = bashPath + "特征数据和生料数据\\";
        DataSampling dataSampling = new DataSampling(expandedName,
                ",",
                sourcePath,targetPath,
                true,IntervalType.HOUR,"UTF-8");

        long start = System.currentTimeMillis();

        dataSampling.dataSampling();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据聚合执行耗时：" + diff + "ms");
        System.out.println("数据聚合执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 数据连接
     *
     */
    public static void step15() {
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\source\\";
        String sourcePath = bashPath;
        String charDataPath = bashPath + "特征数据和生料数据\\";
        String targetPath = bashPath + "join\\";
        DataJoin dataJoin = new DataJoin(expandedName,
                ",",
                sourcePath,targetPath,charDataPath,
                true,"UTF-8");

        long start = System.currentTimeMillis();

        dataJoin.dataJoin();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据滞后处理执行耗时：" + diff + "ms");
        System.out.println("数据滞后处理执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }



}
