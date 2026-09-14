#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
spark-submit get_spark_table_shecme_filter.py  --database example --table "*"  --only-with-comments
spark-submit get_spark_table_shecme_filter.py  --database "*" --table "*"  --only-with-comments  # 支持遍历所有数据库
spark-submit get_spark_table_shecme_filter.py  --database "*" --table "*"  --only-with-comments --check-data  # 检查最近分区是否有数据

获取Spark表结构并保存为文档格式

该脚本使用Spark SQL客户端执行SHOW CREATE TABLE命令，
获取指定表的详细结构信息，并将其保存为文档格式，
包括表的列信息、分区信息、存储格式以及中文备注。
"""

import os
import re
import json
import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

def get_table_schema(spark, database, table_name):
    """
    获取表的DDL语句和结构信息
    
    Args:
        spark: SparkSession对象
        database: 数据库名称
        table_name: 表名称
        
    Returns:
        dict: 包含表结构信息的字典
    """
    # 设置当前数据库
    spark.sql(f"USE {database}")
    
    # 尝试获取表的创建语句
    try:
        # 首先尝试使用AS SERDE选项
        create_table_df = spark.sql(f"SHOW CREATE TABLE {database}.{table_name}")
        create_table_stmt = create_table_df.collect()[0][0]
    except Exception as e:
        print(f"警告: 使用SERDE选项获取建表语句失败: {str(e)}")
        try:
            # 如果失败，尝试不带AS SERDE选项
            create_table_df = spark.sql(f"SHOW CREATE TABLE {database}.{table_name}  AS SERDE")
            create_table_stmt = create_table_df.collect()[0][0]
        except Exception as e2:
            print(f"警告: 获取建表语句失败: {str(e2)}")
            create_table_stmt = f"-- 无法获取 {database}.{table_name} 的建表语句"
    
    # 获取表的详细信息
    try:
        table_info_df = spark.sql(f"DESCRIBE EXTENDED {database}.{table_name}")
        table_info = table_info_df.collect()
    except Exception as e:
        print(f"警告: 获取表描述失败: {str(e)}")
        # 创建一个空的表信息列表
        table_info = []
    
    # 解析列信息和注释
    columns = []
    properties = {}
    current_section = "columns"
    table_comment = ""
    
    for row in table_info:
        col_name = row.col_name.strip() if hasattr(row, "col_name") else ""
        data_type = row.data_type if hasattr(row, "data_type") else ""
        comment = row.comment if hasattr(row, "comment") and row.comment else ""
        
        if col_name == "# Detailed Table Information":
            current_section = "properties"
            continue
            
        if current_section == "columns":
            if not col_name.startswith("#"):
                columns.append({
                    "name": col_name,
                    "type": data_type,
                    "comment": comment
                })
        elif current_section == "properties" and col_name and data_type:
            properties[col_name] = data_type
            # 尝试获取表注释（兼容 Comment/comment 等大小写差异）
            if col_name.strip().lower() in {"comment", "table comment"}:
                table_comment = data_type
    
    # 提取存储格式
    storage_format = "未知"
    location = "未知"
    serde_lib = "未知"
    
    for key, value in properties.items():
        if key == "InputFormat":
            if "parquet" in value.lower():
                storage_format = "Parquet"
            elif "orc" in value.lower():
                storage_format = "ORC"
            elif "textinputformat" in value.lower():
                storage_format = "Text"
            elif "json" in value.lower():
                storage_format = "JSON"
            elif "avro" in value.lower():
                storage_format = "Avro"
            else:
                storage_format = value  # 保存原始值
            
        if key == "Location":
            location = value
            
        if key == "SerDe Library" or key == "serde":
            serde_lib = value

    # 兜底：若 DESCRIBE EXTENDED 未提取到表注释，尝试从建表语句中提取
    if not table_comment and create_table_stmt:
        table_comment_match = re.search(
            r"\nCOMMENT\s+'((?:[^'\\]|\\.)*)'",
            create_table_stmt,
            re.IGNORECASE
        )
        if table_comment_match:
            table_comment = table_comment_match.group(1).replace("\\'", "'")
    
    # 尝试从属性或创建语句中提取分区信息
    partition_info = []
    
    # 从创建语句中提取
    partition_pattern = r'PARTITIONED BY \((.*?)\)'
    partition_match = re.search(partition_pattern, create_table_stmt, re.DOTALL)
    
    if partition_match:
        partition_cols_str = partition_match.group(1)
        # 匹配格式为 `col_name` data_type COMMENT 'comment' 的列定义
        partition_cols = re.findall(r'`(.*?)`\s+(.*?)(?:COMMENT\s+\'(.*?)\')?(?:,|\n|$)', partition_cols_str, re.DOTALL)
        
        for col in partition_cols:
            name = col[0]
            data_type = col[1].strip()
            comment = col[2] if len(col) > 2 and col[2] else ""
            
            partition_info.append({
                "name": name,
                "type": data_type,
                "comment": comment
            })
    
    # 如果从创建语句中无法提取，尝试从属性中查找
    if not partition_info:
        for key, value in properties.items():
            if key == "Partition Keys":
                try:
                    # 尝试解析分区键信息
                    # 格式可能是 [col_name:data_type, col_name:data_type]
                    parts = re.findall(r'(\w+):(\w+)', value)
                    for name, data_type in parts:
                        partition_info.append({
                            "name": name,
                            "type": data_type,
                            "comment": ""
                        })
                except Exception as e:
                    print(f"警告: 解析分区键失败: {str(e)}")
    
    # 构建结果
    result = {
        "database": database,
        "table_name": table_name,
        "table_comment": table_comment,
        "columns": columns,
        "partition_columns": partition_info,
        "storage_format": storage_format,
        "serde_lib": serde_lib,
        "location": location,
        "properties": properties,
        "create_statement": create_table_stmt
    }
    
    return result

def save_schema_as_markdown(schema_info, output_dir):
    """
    将表结构信息保存为Markdown格式
    
    Args:
        schema_info: 表结构信息字典
        output_dir: 输出目录
    """
    db_name = schema_info["database"]
    table_name = schema_info["table_name"]
    table_comment = schema_info["table_comment"]
    
    # 创建Markdown输出目录
    md_output_dir = os.path.join(output_dir, "markdown")
    os.makedirs(md_output_dir, exist_ok=True)
    
    # 构建文件名
    file_name = f"{db_name}.{table_name}.md"
    file_path = os.path.join(md_output_dir, file_name)
    
    with open(file_path, "w", encoding="utf-8") as f:
        # 写入标题
        f.write(f"# {db_name}.{table_name} 表结构文档\n\n")
        if table_comment:
            f.write(f"**表说明**: {table_comment}\n\n")
        f.write(f"*文档生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n\n")
        
        # 基本信息
        f.write("## 基本信息\n\n")
        f.write(f"- **数据库名**: {db_name}\n")
        f.write(f"- **表名**: {table_name}\n")
        if table_comment:
            f.write(f"- **表说明**: {table_comment}\n")
        f.write(f"- **存储格式**: {schema_info['storage_format']}\n")
        f.write(f"- **SerDe库**: {schema_info['serde_lib']}\n")
        f.write(f"- **存储位置**: {schema_info['location']}\n\n")
        
        # 列信息
        f.write("## 字段信息\n\n")
        f.write("| 序号 | 字段名 | 数据类型 | 中文说明 |\n")
        f.write("| ---- | ------ | -------- | -------- |\n")
        
        for i, col in enumerate(schema_info["columns"], 1):
            f.write(f"| {i} | {col['name']} | {col['type']} | {col['comment']} |\n")
        
        # 分区信息
        if schema_info["partition_columns"]:
            f.write("\n## 分区字段\n\n")
            f.write("| 序号 | 分区字段 | 数据类型 | 中文说明 |\n")
            f.write("| ---- | -------- | -------- | -------- |\n")
            
            for i, col in enumerate(schema_info["partition_columns"], 1):
                f.write(f"| {i} | {col['name']} | {col['type']} | {col['comment']} |\n")
        
        # 创建语句
        f.write("\n## 建表语句\n\n")
        f.write("```sql\n")
        f.write(schema_info["create_statement"])
        f.write("\n```\n")
        
        # 其他属性
        f.write("\n## 其他属性\n\n")
        f.write("```\n")
        for key, value in schema_info["properties"].items():
            f.write(f"{key}: {value}\n")
        f.write("```\n")
    
    print(f"表结构文档已保存到: {file_path}")

def save_schema_as_json(schema_info, output_dir):
    """
    将表结构信息保存为JSON格式
    
    Args:
        schema_info: 表结构信息字典
        output_dir: 输出目录
    """
    db_name = schema_info["database"]
    table_name = schema_info["table_name"]
    
    # 创建JSON输出目录
    json_output_dir = os.path.join(output_dir, "json")
    os.makedirs(json_output_dir, exist_ok=True)
    
    # 构建文件名
    file_name = f"{db_name}.{table_name}.json"
    file_path = os.path.join(json_output_dir, file_name)
    
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(schema_info, f, ensure_ascii=False, indent=2)
    
    print(f"表结构JSON已保存到: {file_path}")

def has_chinese_comment(schema_info):
    """
    检查表或字段是否有中文注释
    
    Args:
        schema_info: 表结构信息字典
        
    Returns:
        tuple: (是否有表注释, 有中文注释的字段数)
    """
    # 检查是否有表注释
    has_table_comment = bool(schema_info["table_comment"] and re.search(r'[\u4e00-\u9fff]', schema_info["table_comment"]))
    
    # 计算有中文注释的字段数
    fields_with_chinese_comment = 0
    
    # 检查普通字段
    for col in schema_info["columns"]:
        if col["comment"] and re.search(r'[\u4e00-\u9fff]', col["comment"]):
            fields_with_chinese_comment += 1
            
    # 检查分区字段
    for col in schema_info["partition_columns"]:
        if col["comment"] and re.search(r'[\u4e00-\u9fff]', col["comment"]):
            fields_with_chinese_comment += 1
            
    return (has_table_comment, fields_with_chinese_comment)

def check_recent_data(spark, database, table_name, schema_info, days_back=1):
    """
    检查表的最近T-1分区是否有数据
    
    Args:
        spark: SparkSession对象
        database: 数据库名称
        table_name: 表名称
        schema_info: 表结构信息
        days_back: 检查最近几天的分区
        
    Returns:
        tuple: (是否有数据, 检查结果信息)
    """
    partition_columns = schema_info["partition_columns"]
    
    # 如果没有分区列，直接检查表是否有数据
    if not partition_columns:
        try:
            count_df = spark.sql(f"SELECT COUNT(*) as cnt FROM {database}.{table_name} LIMIT 1")
            count = count_df.collect()[0].cnt
            has_data = count > 0
            info = f"表 {database}.{table_name} 没有分区，总行数检查: {'有数据' if has_data else '无数据'}"
            return has_data, info
        except Exception as e:
            return False, f"检查表数据时出错: {str(e)}"
    
    # 查找日期类型的分区列
    date_partitions = []
    for col in partition_columns:
        col_name = col["name"]
        col_type = col["type"].lower()
        # 日期类型的列名通常包含 dt, date, day 等
        if ("date" in col_name.lower() or "dt" in col_name.lower() or "day" in col_name.lower()) and \
           ("string" in col_type or "varchar" in col_type or "char" in col_type):
            date_partitions.append(col_name)
    
    if not date_partitions:
        # 如果没有找到日期分区列，尝试查询所有分区
        try:
            partitions_df = spark.sql(f"SHOW PARTITIONS {database}.{table_name}")
            partitions = [row.partition for row in partitions_df.collect()]
            
            if not partitions:
                return False, f"表 {database}.{table_name} 没有找到分区"
            
            # 获取最新的分区
            latest_partition = partitions[-1]
            
            # 检查最新分区是否有数据
            count_df = spark.sql(f"SELECT COUNT(*) as cnt FROM {database}.{table_name} WHERE {latest_partition.replace('/', ' AND ')} LIMIT 1")
            count = count_df.collect()[0].cnt
            has_data = count > 0
            info = f"表 {database}.{table_name} 最新分区 {latest_partition} {'有数据' if has_data else '无数据'}"
            return has_data, info
            
        except Exception as e:
            return False, f"检查分区数据时出错: {str(e)}"
    
    # 使用找到的日期分区列
    main_date_partition = date_partitions[0]
    
    # 计算最近的日期分区
    today = datetime.now()
    check_date = today - timedelta(days=days_back)
    date_str = check_date.strftime('%Y%m%d')
    
    try:
        # 检查最近的分区是否存在
        partition_exists_df = spark.sql(f"SHOW PARTITIONS {database}.{table_name} PARTITION({main_date_partition}='{date_str}')")
        partition_exists = len(partition_exists_df.collect()) > 0
        
        if not partition_exists:
            # 如果指定日期的分区不存在，尝试获取所有分区并找到最近的
            all_partitions_df = spark.sql(f"SHOW PARTITIONS {database}.{table_name}")
            all_partitions = [row.partition for row in all_partitions_df.collect()]
            
            if not all_partitions:
                return False, f"表 {database}.{table_name} 没有找到分区"
            
            # 获取最新的分区
            latest_partition = all_partitions[-1]
            
            # 检查最新分区是否有数据
            count_df = spark.sql(f"SELECT COUNT(*) as cnt FROM {database}.{table_name} WHERE {latest_partition.replace('/', ' AND ')} LIMIT 1")
            count = count_df.collect()[0].cnt
            has_data = count > 0
            info = f"表 {database}.{table_name} 最新分区 {latest_partition} {'有数据' if has_data else '无数据'}"
            return has_data, info
        
        # 检查指定分区是否有数据
        count_df = spark.sql(f"SELECT COUNT(*) as cnt FROM {database}.{table_name} WHERE {main_date_partition}='{date_str}' LIMIT 1")
        count = count_df.collect()[0].cnt
        has_data = count > 0
        info = f"表 {database}.{table_name} 分区 {main_date_partition}='{date_str}' {'有数据' if has_data else '无数据'}"
        return has_data, info
        
    except Exception as e:
        return False, f"检查分区数据时出错: {str(e)}"

def save_statistics(tables_stats, output_dir):
    """
    保存统计信息到文件
    
    Args:
        tables_stats: 表统计信息列表
        output_dir: 输出目录
    """
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 统计总数
    total_tables = len(tables_stats)
    tables_with_chinese_comment = sum(1 for stats in tables_stats if stats["has_table_comment"])
    total_fields_with_chinese_comment = sum(stats["fields_with_chinese_comment"] for stats in tables_stats)
    tables_with_recent_data = sum(1 for stats in tables_stats if stats.get("has_recent_data", True))
    
    # 生成统计报告文件
    stats_file = os.path.join(output_dir, "chinese_comments_statistics.md")
    
    with open(stats_file, "w", encoding="utf-8") as f:
        # 写入统计摘要
        f.write("# 数据库表中文注释统计报告\n\n")
        f.write(f"*报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n\n")
        
        f.write("## 统计摘要\n\n")
        f.write(f"- **总表数**: {total_tables}\n")
        f.write(f"- **有中文表注释的表数**: {tables_with_chinese_comment} ({tables_with_chinese_comment/total_tables*100:.2f}%)\n")
        f.write(f"- **有中文字段注释的字段总数**: {total_fields_with_chinese_comment}\n")
        f.write(f"- **有最近数据的表数**: {tables_with_recent_data} ({tables_with_recent_data/total_tables*100:.2f}%)\n\n")
        
        # 写入详细表格
        f.write("## 详细统计\n\n")
        f.write("| 序号 | 数据库名 | 表名 | 表注释 | 有中文注释的字段数 | 最近有数据 | 数据检查结果 |\n")
        f.write("| ---- | -------- | ---- | ------ | ------------------ | ---------- | ------------ |\n")
        
        for i, stats in enumerate(tables_stats, 1):
            has_recent_data = stats.get("has_recent_data", True)
            data_check_info = stats.get("data_check_info", "未检查")
            f.write(f"| {i} | {stats['database']} | {stats['table_name']} | {'✓' if stats['has_table_comment'] else '✗'} | {stats['fields_with_chinese_comment']} | {'✓' if has_recent_data else '✗'} | {data_check_info} |\n")
    
    # 生成JSON格式的统计数据
    stats_json_file = os.path.join(output_dir, "chinese_comments_statistics.json")
    
    with open(stats_json_file, "w", encoding="utf-8") as f:
        stats_data = {
            "generated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "total_tables": total_tables,
            "tables_with_chinese_comment": tables_with_chinese_comment,
            "total_fields_with_chinese_comment": total_fields_with_chinese_comment,
            "tables_with_recent_data": tables_with_recent_data,
            "tables": tables_stats
        }
        json.dump(stats_data, f, ensure_ascii=False, indent=2)
    
    print(f"统计报告已保存到: {stats_file}")
    print(f"统计数据已保存到: {stats_json_file}")
    print(f"总表数: {total_tables}, 有中文表注释的表数: {tables_with_chinese_comment}, 有中文字段注释的字段总数: {total_fields_with_chinese_comment}, 有最近数据的表数: {tables_with_recent_data}")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="获取Spark表结构并保存为文档")
    parser.add_argument("--database", required=True, help="数据库名称，支持'*'表示所有数据库")
    parser.add_argument("--table", required=True, help="表名或表名模式(支持*)") 
    parser.add_argument("--output", default="./table_filter_schemas", help="输出根目录")
    parser.add_argument("--format", choices=["markdown", "json", "both"], default="both", 
                        help="输出格式: markdown, json 或 both")
    parser.add_argument("--md-path", default="markdown", help="Markdown文件子目录名")
    parser.add_argument("--json-path", default="json", help="JSON文件子目录名")
    parser.add_argument("--only-with-comments", action="store_true", help="只处理有中文注释的表")
    parser.add_argument("--check-data", action="store_true", help="检查表最近分区是否有数据")
    parser.add_argument("--days-back", type=int, default=1, help="检查几天前的分区数据，默认为1")
    args = parser.parse_args()

    # 创建输出根目录
    os.makedirs(args.output, exist_ok=True)

    # 初始化Spark会话
    spark = SparkSession.builder \
        .appName("GetSparkTableSchema") \
        .enableHiveSupport() \
        .getOrCreate()
    
    try:
        # 确定要处理的数据库列表
        all_databases = []
        if args.database == "*":
            # 获取所有数据库列表
            print("获取所有数据库列表...")
            databases_df = spark.sql("SHOW DATABASES")
            all_databases = [row.databaseName for row in databases_df.collect()]
            print(f"发现 {len(all_databases)} 个数据库")
        else:
            all_databases = [args.database]
            
        # 统计信息
        all_tables_stats = []
        total_processed_tables = 0
        total_chinese_tables = 0
        total_chinese_fields = 0
        total_tables_with_data = 0
        
        # 遍历每个数据库
        for db_name in all_databases:
            print(f"\n处理数据库: {db_name}")
            
            try:
                # 获取匹配的表列表
                if '*' in args.table:
                    pattern = args.table.replace('*', '.*')
                    tables_df = spark.sql(f"SHOW TABLES IN {db_name}")
                    tables = [row.tableName for row in tables_df.collect() 
                             if re.match(pattern, row.tableName)]
                else:
                    tables = [args.table]
                
                if not tables:
                    print(f"没有找到匹配的表: {db_name}.{args.table}")
                    continue
                    
                print(f"找到 {len(tables)} 个匹配的表")
                
                # 统计信息
                tables_stats = []
                tables_with_chinese_comment = 0
                chinese_comment_fields = 0
                processed_tables = 0
                tables_with_data = 0
                
                # 处理每个表
                for table_name in tables:
                    print(f"处理表: {db_name}.{table_name}")
                    
                    try:
                        # 获取表结构
                        schema_info = get_table_schema(spark, db_name, table_name)
                        
                        # 检查是否有中文注释
                        has_table_comment, fields_with_chinese_comment = has_chinese_comment(schema_info)
                        
                        # 检查最近分区是否有数据
                        has_recent_data = True
                        data_check_info = "未检查"
                        
                        if args.check_data:
                            print(f"检查表 {db_name}.{table_name} 最近分区数据...")
                            has_recent_data, data_check_info = check_recent_data(spark, db_name, table_name, schema_info, args.days_back)
                            print(f"数据检查结果: {data_check_info}")
                            
                            if has_recent_data:
                                tables_with_data += 1
                                total_tables_with_data += 1
                        
                        # 添加统计信息
                        stats = {
                            "database": db_name,
                            "table_name": table_name,
                            "has_table_comment": has_table_comment,
                            "fields_with_chinese_comment": fields_with_chinese_comment,
                            "has_recent_data": has_recent_data,
                            "data_check_info": data_check_info
                        }
                        tables_stats.append(stats)
                        all_tables_stats.append(stats)
                        
                        # 更新统计
                        if has_table_comment:
                            tables_with_chinese_comment += 1
                            total_chinese_tables += 1
                        chinese_comment_fields += fields_with_chinese_comment
                        total_chinese_fields += fields_with_chinese_comment
                        
                        # 如果只处理有中文注释的表，且当前表没有中文注释，则跳过
                        if args.only_with_comments and not (has_table_comment or fields_with_chinese_comment > 0):
                            print(f"表 {db_name}.{table_name} 没有中文注释，跳过处理")
                            continue
                            
                        # 如果要检查数据，且当前表最近分区没有数据，则跳过
                        if args.check_data and not has_recent_data:
                            print(f"表 {db_name}.{table_name} 最近分区没有数据，跳过处理")
                            continue
                        
                        # 保存文档
                        if args.format in ["markdown", "both"]:
                            save_schema_as_markdown(schema_info, args.output)
                        
                        if args.format in ["json", "both"]:
                            save_schema_as_json(schema_info, args.output)
                        
                        processed_tables += 1
                        total_processed_tables += 1
                        print(f"表 {db_name}.{table_name} 处理完成")
                    except Exception as e:
                        print(f"处理表 {db_name}.{table_name} 时发生错误: {str(e)}")
                        continue
                
                # 打印数据库级别统计结果
                print(f"\n数据库 {db_name} 统计结果:")
                print(f"表总数: {len(tables)}")
                print(f"有中文表注释的表数: {tables_with_chinese_comment}")
                print(f"有中文字段注释的字段总数: {chinese_comment_fields}")
                if args.check_data:
                    print(f"有最近数据的表数: {tables_with_data}")
                print(f"实际处理的表数: {processed_tables}")
                
            except Exception as e:
                print(f"处理数据库 {db_name} 时发生错误: {str(e)}")
                continue
        
        # 保存全局统计信息
        if all_tables_stats:
            save_statistics(all_tables_stats, args.output)
        
        # 打印全局统计结果
        print(f"\n全局统计结果:")
        print(f"处理的数据库总数: {len(all_databases)}")
        print(f"处理的表总数: {total_processed_tables}")
        print(f"有中文表注释的表总数: {total_chinese_tables}")
        print(f"有中文字段注释的字段总数: {total_chinese_fields}")
        if args.check_data:
            print(f"有最近数据的表总数: {total_tables_with_data}")
            
    except Exception as e:
        print(f"发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
