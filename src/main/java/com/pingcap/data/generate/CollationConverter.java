package com.pingcap.data.generate;

import java.io.*;
import java.nio.file.*;
import java.util.regex.*;

public class CollationConverter {


    
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("用法: java CollationConverter <输入SQL文件> <输出SQL文件>");
            System.out.println("示例: java CollationConverter schema.sql schema_fixed.sql");
            return;
        }
        
        String inputFile = args[0];
        String outputFile = args[1];
        
        try {
            convertCollation(inputFile, outputFile);
            System.out.println("转换完成！输出文件: " + outputFile);
        } catch (IOException e) {
            System.err.println("文件操作错误: " + e.getMessage());
        }
    }
    
    public static void convertCollation(String inputFile, String outputFile) throws IOException {
        Path outputPath = Paths.get(outputFile);

        File input = new File(inputFile);
        StringBuffer stringBuffer = new StringBuffer();
        if (input.isDirectory()) {
            File[] dirFiles = input.listFiles();
            for (File file : dirFiles){
                if (file.isFile()){
                    // 读取SQL文件
                    String content = new String(Files.readAllBytes(file.toPath()));
                    // 处理CREATE TABLE语句
                    stringBuffer.append(processCreateTableStatements(content)).append("\n\n");

                }
            }
            // 写入输出文件
            Files.write(outputPath, stringBuffer.toString().getBytes());
        } else {
            Path inputPath = Paths.get(inputFile);

            // 读取SQL文件
            String content = new String(Files.readAllBytes(inputPath));

            // 处理CREATE TABLE语句
            String processedContent = processCreateTableStatements(content);

            // 写入输出文件
            Files.write(outputPath, processedContent.getBytes());
        }
        

    }
    
    private static String processCreateTableStatements(String sqlContent) {
        // 正则表达式匹配CREATE TABLE语句
        Pattern tablePattern = Pattern.compile(
            "(CREATE TABLE\\s+`?(\\w+)`?\\s*\\([\\s\\S]*?\\)\\s*[^;]*;$)",
            Pattern.CASE_INSENSITIVE
        );
        
        Matcher matcher = tablePattern.matcher(sqlContent);
        StringBuffer result = new StringBuffer();
        
        while (matcher.find()) {
            String createTableStatement = matcher.group(1);
            System.out.println(createTableStatement);
            String tableName = matcher.group(2);
            
            System.out.println("处理表: " + tableName);
            
            // 处理表级别的排序规则
            String processedStatement = processTableLevelCollation(createTableStatement);
            
            matcher.appendReplacement(result, processedStatement);
        }
        
        matcher.appendTail(result);
        return result.toString();
    }
    
    private static String processTableLevelCollation(String createTableStatement) {
        // 检查是否已经指定了字符集和排序规则
        boolean hasCharset = Pattern.compile("CHARSET\\s*=", Pattern.CASE_INSENSITIVE)
                                  .matcher(createTableStatement).find();
        boolean hasCollate = Pattern.compile("COLLATE\\s*=", Pattern.CASE_INSENSITIVE)
                                  .matcher(createTableStatement).find();
        
        // 如果既没有指定字符集也没有指定排序规则
        if (!hasCharset && !hasCollate) {
            // 在ENGINE子句前添加默认字符集和排序规则
            return createTableStatement.replaceAll(
                "(?i)(ENGINE\\s*=)",
                "DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci $1"
            );
        }
        // 如果指定了字符集但没有指定排序规则
        else if (hasCharset && !hasCollate) {
            // 在字符集后添加排序规则
            createTableStatement =  createTableStatement.replaceAll(
                "(?i)(CHARSET\\s*=\\s*[^\\s\\),]+)",
                "$1 COLLATE=utf8mb4_general_ci "
            ).replaceAll(";","") + " ;";
            return createTableStatement;
        }
        // 如果指定了排序规则但不是我们想要的
        else if (hasCollate) {
            // 将现有的排序规则改为utf8mb4_general_ci
            return createTableStatement.replaceAll(
                "(?i)COLLATE\\s*=\\s*[^\\s\\),]+",
                "COLLATE=utf8mb4_general_ci"
            );
        }
        
        return createTableStatement;
    }
    


}