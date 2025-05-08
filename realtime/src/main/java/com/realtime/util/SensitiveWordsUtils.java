package com.realtime.util;

import java.io.*;
import java.util.ArrayList;

public class SensitiveWordsUtils {

    //keypoint 读取文件内容,并且逐行添加到list集合中,返回list
    public static ArrayList<String> getSensitiveWordsLists(){
        //创建ArrayList
        ArrayList<String> res = new ArrayList<>();
        //try-with-resources,带资源的 try 语句, 自动关闭文件、数据库、网络等资源操作
        //读取文件内容,并逐行添加到方法中创建的ArrayList中
        try(InputStream is = SensitiveWordsUtils.class.getClassLoader().getResourceAsStream("Identify-sensitive-words.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(is,"utf-8"))
        ){
            String line;
            while ((line = reader.readLine()) != null) {
                res.add(line);
            }
        }catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //返回list
        return res;
    }
}
