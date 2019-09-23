package data.clean.core;

import java.io.*;

/**
 * 文件列合并，确保数据一致再去合并
 */
public class ColumnMerge {

    //扩展名
    public String expandedName;
    //分隔符
    public String separatorChars;
    //原路径
    public String sourcePath;
    //目标路径
    public String targetPath;
    //是否存在标题
    public boolean existTitle;
    //源文件编码
    public String charset;

    public ColumnMerge(String expandedName, String separatorChars, String sourcePath, String targetPath,
                                        boolean existTitle, String charset) {
        this.expandedName = expandedName;
        this.separatorChars = separatorChars;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.existTitle = existTitle;
        this.charset = charset;
    }

    /**
     * 列合并
     */
    public void columnMerge() {
        File file = new File(sourcePath);
        if (file.exists()) {
            File[] files = file.listFiles();
            if (null == files || files.length == 0) {
                System.out.println("文件夹是空的!");
                return;
            } else {
                int mergeTimes = 0;
                for (File file2 : files) {
                    mergeTimes++;
                    if (file2.isDirectory()) {
                        continue;
                    } else {
                        columnMerge(file2,"bitMerge" + mergeTimes + ".csv",mergeTimes);
                        System.out.println("文件:" + file2.getAbsolutePath() + " 处理完成");
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
    }

    public void columnMerge(File file,String targetFileName,int mergeTimes) {
        //文件流
        FileInputStream stream = null;
        //读文件对象
        BufferedReader reader = null;
        //文件流
        FileInputStream streamTarget = null;
        //读文件对象
        BufferedReader readerTarget = null;
        //写文件对象
        FileWriter writer = null;
        File frontFile = null;
        try {
            stream = new FileInputStream(file);
            String pathFile = targetPath + targetFileName;
            File targetPathFile = new File(targetPath);
            File targetFile = new File(pathFile);
            if(!targetPathFile.exists()){
                targetPathFile.mkdirs();
            }

            if(!targetFile.exists()){
                targetFile.createNewFile();
            }


            if (mergeTimes > 1) {
                frontFile = new File(targetPath + "bitMerge" + (mergeTimes - 1) + ".csv");
                streamTarget = new FileInputStream(frontFile);
                readerTarget = new BufferedReader(new InputStreamReader(streamTarget));
            }

            writer = new FileWriter(targetFile,true);
            reader = new BufferedReader(new InputStreamReader(stream));

            String str = null;
            String strTarget = null;
            if (mergeTimes > 1) {
                while ((str = reader.readLine()) != null && (strTarget = readerTarget.readLine()) != null) {
                    writer.write(strTarget + str.substring(str.indexOf(",")) + "\n");
                }
            } else {
                while ((str = reader.readLine()) != null) {
                    writer.write(str + "\n");
                }
            }


        } catch (IOException ioe) {
            ioe.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stream != null) {
                    stream.close();
                }
                if (reader != null) {
                    reader.close();
                }
                if (streamTarget != null) {
                    streamTarget.close();
                }
                if (readerTarget != null) {
                    readerTarget.close();
                }
                if (null != writer) {
                    writer.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (frontFile != null) {
            frontFile.delete();
        }
    }

}
