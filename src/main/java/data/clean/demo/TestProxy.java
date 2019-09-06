package data.clean.demo;

import data.clean.proxy.FileRecursionProxy;

public class TestProxy {

    public void save(String path,String expandedName) {
    }

    public static void main(String[] args) {
        TestProxy testProxy = new TestProxy();
        TestProxy proxy = (TestProxy) new FileRecursionProxy(testProxy).getTargetInstance();
        String path = "E:\\work\\天数\\数据清洗\\红狮数据\\漳平三期4月1日起新数据-张居宾\\位号数据\\";
        String expandedName = ".+\\.csv";
        proxy.save(path,expandedName);
    }
}
