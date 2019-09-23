package data.clean.core;

import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 文件列拆分，多列拆成一个新的文件
 */
public class ColumnSplit {

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
    //拆分列
    public String[] columns;

    public ColumnSplit(String expandedName, String separatorChars, String sourcePath, String targetPath,
                       boolean existTitle, String charset,String[] columns) {
        this.expandedName = expandedName;
        this.separatorChars = separatorChars;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.existTitle = existTitle;
        this.charset = charset;
        this.columns = columns;
    }

    /**
     * 列拆分
     */
    public void columnSplit() {
        File file = new File(sourcePath);
        columnSplit(file);
        System.out.println("文件:" + file.getAbsolutePath() + " 处理完成");
    }

    public void columnSplit(File file) {
        //文件流
        FileInputStream stream = null;
        //读文件对象
        BufferedReader reader = null;
        //写文件对象
        FileWriter writer = null;
        File frontFile = null;
        try {
            stream = new FileInputStream(file);
            reader = new BufferedReader(new InputStreamReader(stream));
            File targetFile = new File(targetPath);
            if(targetFile.exists()){
                targetFile.delete();
            }
            targetFile.createNewFile();

            writer = new FileWriter(targetFile,true);

            String str = null;
            int counter = 0;
            List<Integer> posList = new ArrayList();
            posList.add(0);
            while ((str = reader.readLine()) != null) {
                counter++;
                String[] strings = StringUtils.splitPreserveAllTokens(str,separatorChars);
                if (counter == 1) {
                    StringBuffer head = new StringBuffer("时间");
                    for (int i = 0; i < columns.length; i++) {
                        String col = columns[i];
                        boolean found = false;
                        for (int j = 0; j < strings.length; j++) {
                            String matchStr = strings[j].toUpperCase();
                            if (matchStr.contains(col)) {
                                found = true;
                                posList.add(j);
                                head.append(separatorChars).append(matchStr);
                                break;
                            }
                        }
                        if (!found) {
                            throw new RuntimeException(col + " not found");
                        }
                    }
                    head.append("\n");
                    writer.write(head.toString());
                } else {
                    StringBuffer value = new StringBuffer();
                    for (int i = 0; i < posList.size(); i++) {
                        if (i == posList.size() - 1) {
                            value.append(strings[posList.get(i)]).append("\n");
                        } else {
                            value.append(strings[posList.get(i)]).append(separatorChars);
                        }
                    }
                    writer.write(value.toString());
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
