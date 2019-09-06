package data.clean.proxy;

import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;

import java.io.File;
import java.lang.reflect.Method;

public class FileRecursionProxy implements MethodInterceptor {

    private Object target;

    public FileRecursionProxy(Object target) {
        this.target = target;
    }

    /**
     * 获取代理实例
     * @return
     */
    public Object getTargetInstance() {
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(target.getClass());
        enhancer.setCallback(this);
        return enhancer.create();
    }

    public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
        System.out.println("begin");
        File file = new File(objects[0].toString());
        if (file.exists()) {
            File[] files = file.listFiles();
            if (null == files || files.length == 0) {
                System.out.println("文件夹是空的!");
                return null;
            } else {
                for (File file2 : files) {
                    if (file2.isDirectory()) {
                        continue;
                    } else {
                        if (file2.getAbsolutePath().matches(objects[1].toString())) {
                            method.invoke(target,objects);
                            System.out.println("文件:" + file2.getAbsolutePath() + " 处理完成");
                        }
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
        System.out.println("end");
        return null;
    }
}
