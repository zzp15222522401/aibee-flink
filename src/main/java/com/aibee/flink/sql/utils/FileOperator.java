package com.aibee.flink.sql.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 文件操作工具类
 */
public class FileOperator {
    /**
     * 读取传入文件所在路径，以字符串类型 返回该文件中的内容
     * @param fileStr 文件路径
     * @return 文件中的内容
     */
    public static String readTextFile(String fileStr){
        String str = null;
        try {
            Stream<String> stream = Files.lines(Paths.get(fileStr));
            str = stream.collect(Collectors.joining("\n"));
        } catch (IOException e) {
            //Your exception handling here
            e.printStackTrace();
        }
        return str;
    }
}
