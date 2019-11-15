package data.clean.business.ZhangPingPhase3.data_201909_201911;

import data.clean.common.enums.AggrType;
import data.clean.common.enums.IntervalType;
import data.clean.core.*;

import java.io.File;

public class DataCleaning201909_201911 {


    public static void main(String[] args) {

//        step00(); //文件合并
//        step01();//秒级数据按平均值聚合成分钟级数据
//        step02_1();//篦冷机数据拆分
//        step02_2();//预热器、分解炉数据拆分
//        step02_3();//回转窑数据拆分
//        step03();//位号数据DateFormat
//        step04_1();//篦冷机数据聚合
//        step04_2();//预热器、分解炉数据聚合
//        step04_3();//回转窑数据聚合
//        step05();//原料数据DateFormat
//        step06();//煤粉数据不同位置数据按行做平局聚合
//        step07();//原料数据抽取至同一文件统一处理（手动操作）：step-07-原料数据
//        step08();//原料数据滞后处理（加60分钟）
//        step09();//数据连接位号数据按小时采样
//        step10();//数据整理--位号、原料、熟料数据整理待连接（手动操作）：step-10-数据整理
        step11();//数据连接
    }


    /**
     * 文件合并
     */
    public static void step00() {
        long start = System.currentTimeMillis();

        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平红狮三期201909-201911数据\\";
        String sourcePath = bashPath + "漳平3#实时参数\\9月参数\\";
        String targetPath = bashPath + "\\清洗\\step-00-位号数据：合并\\09-11\\";
        String mergeFile = targetPath + "09.csv";
        NFileRowMerge nFileRowMerge = new NFileRowMerge(expandedName,
                ",",
                sourcePath,targetPath,mergeFile,
                true,"GBK");
        nFileRowMerge.fileMerge();


        sourcePath = bashPath + "漳平3#实时参数\\10月参数\\";
        mergeFile = targetPath + "10.csv";
        nFileRowMerge = new NFileRowMerge(expandedName,
                ",",
                sourcePath,targetPath,mergeFile,
                true,"GBK");
        nFileRowMerge.fileMerge();

        sourcePath = bashPath + "漳平3#实时参数\\11月参数\\";
        mergeFile = targetPath + "11.csv";
        nFileRowMerge = new NFileRowMerge(expandedName,
                ",",
                sourcePath,targetPath,mergeFile,
                true,"GBK");
        nFileRowMerge.fileMerge();

        sourcePath = targetPath;
        targetPath = bashPath + "\\清洗\\step-00-位号数据：合并\\";
        mergeFile = targetPath + "9-11.csv";
        nFileRowMerge = new NFileRowMerge(expandedName,
                ",",
                sourcePath,targetPath,mergeFile,
                true,"utf-8");
        nFileRowMerge.fileMerge();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据聚合执行耗时：" + diff + "ms");
        System.out.println("数据聚合执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 秒级数据按平均值聚合成分钟级数据
     */
    public static void step01() {
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平红狮三期201909-201911数据\\清洗\\";
        String sourcePath = bashPath + "step-00-位号数据：合并\\";
        String targetPath = bashPath + "step-01-位号数据：秒级数据聚合成分钟级数据\\";
        DataAggregationSecond2Minute dataAggregation = new DataAggregationSecond2Minute(expandedName,
                ",",
                sourcePath,targetPath,
                true,60,AggrType.MEDIAN,1,"utf-8");

        long start = System.currentTimeMillis();

        dataAggregation.dataAggregation();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据聚合执行耗时：" + diff + "ms");
        System.out.println("数据聚合执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }


    /**
     * 篦冷机数据拆分
     */
    public static void step02_1() {
        String columnsStr = "篦冷机风机电流06\n" +
                "篦冷机风机电流05\n" +
                "篦冷机风机电流04\n" +
                "篦冷机风机电流03\n" +
                "篦冷机风机电流02\n" +
                "篦冷机风机电流01\n" +
                "篦冷机风机12\n" +
                "篦冷机风机11\n" +
                "篦冷机风机10\n" +
                "篦冷机风机09\n" +
                "篦冷机风机08\n" +
                "篦冷机风机07\n" +
                "篦冷机风机速度06\n" +
                "篦冷机风机速度05\n" +
                "篦冷机风机速度04\n" +
                "篦冷机风机速度03\n" +
                "篦冷机风机速度02\n" +
                "篦冷机风机速度01\n" +
                "篦冷机风机速度12\n" +
                "篦冷机风机速度11\n" +
                "篦冷机风机速度10\n" +
                "篦冷机风机速度09\n" +
                "篦冷机风机速度08\n" +
                "篦冷机风机速度07";

        String[] columns = columnsStr.split("\\n");
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平红狮三期201909-201911数据\\清洗\\";
        String sourcePath = bashPath + "step-01-位号数据：秒级数据聚合成分钟级数据\\9-11.csv";
        String targetPath = bashPath + "step-02-位号数据：数据拆分\\";
        File file = new File(targetPath);
        if (!file.exists()) {
            file.mkdirs();
        }
        targetPath = bashPath + "step-02-位号数据：数据拆分\\篦冷机.csv";
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
    public static void step02_2() {
        String columnsStr = "二次风温\n" +
                "三次风温\n" +
                "烟室温度\n" +
                "分解炉出口温度\n" +
                "分解炉出口压力\n" +
                "尾排风机电流\n" +
                "尾排风机转速\n" +
                "高温风机电流02\n" +
                "高温风机电流01\n" +
                "高温风机转速02\n" +
                "高温风机转速01\n" +
                "C6B下料管温度\n" +
                "C6B锥部压力\n" +
                "C6A下料管温度\n" +
                "窑尾C6A锥部压力\n" +
                "C5B下料管温度\n" +
                "C5B锥部压力\n" +
                "C5A下料管温度\n" +
                "C5A锥部压力\n" +
                "烟囱出口氧含量";
        String[] columns = columnsStr.split("\\n");
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平红狮三期201909-201911数据\\清洗\\";
        String sourcePath = bashPath + "step-01-位号数据：秒级数据聚合成分钟级数据\\9-11.csv";
        String targetPath = bashPath + "step-02-位号数据：数据拆分\\";
        File file = new File(targetPath);
        if (!file.exists()) {
            file.mkdirs();
        }
        targetPath = bashPath + "step-02-位号数据：数据拆分\\预热器-分解炉.csv";
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
    public static void step02_3() {
        String columnsStr = "窑头负压\n" +
                "窑转速02\n" +
                "窑转速01\n" +
                "窑电流02\n" +
                "窑电流01\n" +
                "头煤喂煤量\n" +
                "尾煤喂煤量01\n" +
                "尾煤喂煤量02\n" +
                "投料量\n" +
                "罗茨风机电流04\n" +
                "罗茨风机电流03\n" +
                "罗茨风机电流02\n" +
                "罗茨风机电流01\n" +
                "罗茨风机速度04\n" +
                "罗茨风机速度03\n" +
                "罗茨风机速度02\n" +
                "罗茨风机速度01";
        String[] columns = columnsStr.split("\\n");
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平红狮三期201909-201911数据\\清洗\\";
        String sourcePath = bashPath + "step-01-位号数据：秒级数据聚合成分钟级数据\\9-11.csv";
        String targetPath = bashPath + "step-02-位号数据：数据拆分\\";
        File file = new File(targetPath);
        if (!file.exists()) {
            file.mkdirs();
        }
        targetPath = bashPath + "step-02-位号数据：数据拆分\\回转窑.csv";

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
     * 位号数据DateFormat
     */
    public static void step03() {
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平红狮三期201909-201911数据\\清洗\\";
        String sourcePath = bashPath + "step-02-位号数据：数据拆分\\";
        String targetPath = bashPath + "step-03-位号数据：formatDate\\";
        boolean existTitle = true;
        long start = System.currentTimeMillis();

        new DateFormatBit(expandedName,bashPath,sourcePath,targetPath,existTitle).formatDirectory();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("FormatDate执行耗时：" + diff + "ms");
        System.out.println("FormatDate执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 篦冷机数据聚合
     * 取前30分钟数据平均值聚合
     */
    public static void step04_1() {
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平红狮三期201909-201911数据\\清洗\\";
        String sourcePath = bashPath + "step-03-位号数据：formatDate\\篦冷机.csv";
        String targetPath = bashPath + "step-04-位号数据：数据聚合\\";
        DataAggregation dataAggregation = new DataAggregation(expandedName,
                ",",
                sourcePath,targetPath,
                true,30,AggrType.MEAN,1,"UTF-8");

        long start = System.currentTimeMillis();

        dataAggregation.dataAggregation(new File(sourcePath));

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据聚合执行耗时：" + diff + "ms");
        System.out.println("数据聚合执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 预热器、分解炉数据聚合
     * 取前70分钟的70条数据，对这70条数据取前20条数据做平均值聚合
     */
    public static void step04_2() {
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平红狮三期201909-201911数据\\清洗\\";
        String sourcePath = bashPath + "step-03-位号数据：formatDate\\预热器-分解炉.csv";
        String targetPath = bashPath + "step-04-位号数据：数据聚合\\";
        DataAggregationEnhance dataAggregation = new DataAggregationEnhance(expandedName,
                ",",
                sourcePath,targetPath,
                true,70,20,AggrType.MEAN,1,"UTF-8");

        long start = System.currentTimeMillis();

        dataAggregation.dataAggregation(new File(sourcePath));

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据聚合执行耗时：" + diff + "ms");
        System.out.println("数据聚合执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 回转窑数据聚合
     * 取前60分钟的60条数据，对这60条数据取前30条数据做平均值聚合
     */
    public static void step04_3() {
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平红狮三期201909-201911数据\\清洗\\";
        String sourcePath = bashPath + "step-03-位号数据：formatDate\\回转窑.csv";
        String targetPath = bashPath + "step-04-位号数据：数据聚合\\";
        DataAggregationEnhance dataAggregation = new DataAggregationEnhance(expandedName,
                ",",
                sourcePath,targetPath,
                true,60,30,AggrType.MEAN,1,"UTF-8");

        long start = System.currentTimeMillis();

        dataAggregation.dataAggregation(new File(sourcePath));

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据聚合执行耗时：" + diff + "ms");
        System.out.println("数据聚合执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }


    /**
     * 原料数据DateFormat
     */
    public static void step05() {
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平红狮三期201909-201911数据\\";
        String sourcePath = bashPath + "出磨生料、入窑生料、熟料、煤粉质量数据\\";
        String targetPath = bashPath + "清洗\\step-05-原料和生料数据：DateFormat\\";
        boolean existTitle = true;
        long start = System.currentTimeMillis();

        new DateFormatMaterial(expandedName,bashPath,sourcePath,targetPath,existTitle).formatDirectory();

        long diff  = System.currentTimeMillis() - start;
        System.out.println("FormatDate执行耗时：" + diff + "ms");
        System.out.println("FormatDate执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }


    /**
     * 煤粉数据不同位置数据按行做平局聚合
     *
     */
    public static void step06() {
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平红狮三期201909-201911数据\\清洗\\";
        String sourcePath = bashPath + "step-05-原料和生料数据：DateFormat\\煤粉.csv";
        String targetPath = bashPath + "step-06-原料数据：煤粉数据聚合\\";
        RowMerge rowMerge = new RowMerge(expandedName,
                ",",
                sourcePath,targetPath,
                2,2,AggrType.MEAN,true,"UTF-8");

        long start = System.currentTimeMillis();

        rowMerge.rowMerge(new File(sourcePath));

        long diff  = System.currentTimeMillis() - start;
        System.out.println("数据聚合执行耗时：" + diff + "ms");
        System.out.println("数据聚合执行耗时：" + diff / 60000 + " m" + diff % 60000 / 1000 + " s");
    }

    /**
     * 原料数据滞后处理（加60分钟）
     *
     */
    public static void step08() {
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平红狮三期201909-201911数据\\清洗\\";
        String sourcePath = bashPath + "step-07-原料数据\\";
        String targetPath = bashPath + "step-08-原料数据：滞后处理（加60分钟）\\";
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
    public static void step09() {
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平红狮三期201909-201911数据\\清洗\\";
        String sourcePath = bashPath + "step-04-位号数据：数据聚合\\";
        String targetPath = bashPath + "step-09-位号数据：按小时采样\\";
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
    public static void step11() {
        String expandedName = ".+\\.csv";
        String bashPath = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平红狮三期201909-201911数据\\清洗\\";
        String sourcePath = bashPath + "step-10-数据整理\\";
        String charDataPath = sourcePath + "特征数据和生料数据\\";
        String targetPath = bashPath + "step-11-数据连接\\";
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
