package data.clean.core;

import java.io.*;

/**
 * 文件列合并，确保数据一致再去合并
 */
public class NFileRowMerge {

    //扩展名
    public String expandedName;
    //分隔符
    public String separatorChars;
    //合并路径
    public String sourcePath;
    //目标路径
    public String targetPath;
    //合并文件
    public String mergeFile;
    //是否存在标题
    public boolean existTitle;
    //源文件编码
    public String charset;
    //正在处理第n个文件
    public int dealingCount = 0;

    public NFileRowMerge(String expandedName, String separatorChars, String sourcePath,String targetPath, String mergeFile,
                         boolean existTitle, String charset) {
        this.expandedName = expandedName;
        this.separatorChars = separatorChars;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.mergeFile = mergeFile;
        this.existTitle = existTitle;
        this.charset = charset;
        mkdirs();
    }

    private void mkdirs() {
        File targetPathFile = new File(targetPath);
        if(!targetPathFile.exists()){
            targetPathFile.mkdirs();
        }
    }
    /**
     * 文件合并
     */
    public void fileMerge(){
        FileWriter fileWriter = null;
        try {
            File dealFile = new File(mergeFile);
            if (dealFile.exists()) {
                dealFile.delete();
            }
            dealFile.createNewFile();
            fileWriter = new FileWriter(dealFile,true);
            File file = new File(sourcePath);
            if (file.exists()) {
                File[] files = file.listFiles();
                if (null == files || files.length == 0) {
                    System.out.println("文件夹是空的!");
                    throw new RuntimeException("文件夹是空的");
                } else {
                    for (File file2 : files) {
                        if (file2.isDirectory()) {
                            continue;
                        } else {
                            fileMerge(file2,fileWriter);
                            System.out.println("文件:" + file2.getAbsolutePath() + " 处理完成");
                        }
                    }
                }
            } else {
                System.out.println("文件不存在!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null !=  fileWriter) {
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     * 文件合并
     */
    public void fileMerge(File file,FileWriter fileWriter) {
        //文件流
        FileInputStream fileInputStream = null;
        //读文件对象
        BufferedReader bufferedReader = null;
        //写文件对象
        try {
            fileInputStream = new FileInputStream(file);
            bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream,charset));
            String str = null;
            int counter = 0;
            while ((str = bufferedReader.readLine()) != null) {
                counter++;
                if (counter == 1 && dealingCount != 0) {
                    continue;
                }
                fileWriter.write(str + "\n");
            }
            dealingCount++;

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
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }


}
