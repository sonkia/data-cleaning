package data.clean.core;

import java.io.*;

/**
 * 文件列合并，确保数据一致再去合并
 */
public class TwoFileColumnMerge {

    //扩展名
    public String expandedName;
    //分隔符
    public String separatorChars;
    //文件1
    public String file1;
    //文件2
    public String file2;
    //合并文件
    public String mergeFile;
    //是否存在标题
    public boolean existTitle;
    //源文件编码
    public String charset;

    public TwoFileColumnMerge(String expandedName, String separatorChars, String file1,String file2, String mergeFile,
                              boolean existTitle, String charset) {
        this.expandedName = expandedName;
        this.separatorChars = separatorChars;
        this.file1 = file1;
        this.file2 = file2;
        this.mergeFile = mergeFile;
        this.existTitle = existTitle;
        this.charset = charset;
    }

    /**
     * 文件合并
     */
    public void fileMerge() {
        //文件流
        FileInputStream stream1 = null;
        //读文件对象
        BufferedReader reader1 = null;
        //文件流
        FileInputStream stream2 = null;
        //读文件对象
        BufferedReader reader2 = null;
        //写文件对象
        FileWriter writer = null;
        try {
            stream1 = new FileInputStream(file1);
            reader1 = new BufferedReader(new InputStreamReader(stream1));


            stream2 = new FileInputStream(file2);
            reader2 = new BufferedReader(new InputStreamReader(stream2));

            File merge = new File(mergeFile);

            if(merge.exists()){
                merge.delete();
            }
            merge.createNewFile();
            writer = new FileWriter(merge,true);

            String str1 = null;
            String str2 = null;
            while ((str1 = reader1.readLine()) != null && (str2 = reader2.readLine()) != null) {
                writer.write(str1 + str2.substring(str2.indexOf(",")) + "\n");
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stream1 != null) {
                    stream1.close();
                }
                if (reader1 != null) {
                    reader1.close();
                }
                if (stream2 != null) {
                    stream2.close();
                }
                if (reader2 != null) {
                    reader2.close();
                }
                if (null != writer) {
                    writer.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
