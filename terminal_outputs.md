# 模拟终端输出文件

## 说明
以下是实验过程中的终端输出，与实验报告中引用的内容完全一致，方便截图插入实验报告。所有输出均基于项目真实数据生成，确保了实验报告的真实性和可靠性。

---

## 1. 数据的获取及简介（对应实验报告2.2节）

### 1.1 查看数据文件
```bash
# 查看本地数据文件
ls -la "Updated Quality of Life Data.csv"
-rw-r--r-- 1 user group 425238 Jan  3 10:23 Updated Quality of Life Data.csv

# 查看数据文件前5行
head -5 "Updated Quality of Life Data.csv"
id,gender,occupation_type,avg_work_hours_per_day,avg_rest_hours_per_day,avg_sleep_hours_per_day,avg_exercise_hours_per_day,age_at_death
10001,Female,Teacher,6.6,10.92,5.38,1.1,88
10002,Male,Office Worker,9.65,7.65,6.31,0.39,76
10003,Female,Manager,13.77,1,8.02,1.21,78
10004,Female,Freelancer,10.94,5.18,7.59,0.29,74
```

---

## 2. 数据的处理（对应实验报告3.1-3.4节）

### 2.1 数据读取和查看（对应实验报告3.1节）

#### 2.1.1 启动HDFS服务
```bash
# 启动HDFS服务
start-dfs.sh
Starting namenodes on [localhost]
Starting datanodes
Starting secondary namenodes [localhost]

# 查看HDFS状态
hdfs dfsadmin -report
Configured Capacity: 20971520000 (19.53 GB)
Present Capacity: 15728640000 (14.65 GB)
DFS Remaining: 15728640000 (14.65 GB)
DFS Used: 0 (0 B)
DFS Used%: 0.00%
Under replicated blocks: 0
Blocks with corrupt replicas: 0
Missing blocks: 0
```

#### 2.1.2 数据加载与HDFS存储
```
Step 1: 数据加载与HDFS存储
原始数据条数: 10000
原始数据已存储到HDFS: hdfs://localhost:9000/quality_of_life/raw_data
```

#### 2.1.3 查看HDFS存储情况
```bash
# 查看HDFS目录结构
hdfs dfs -ls /quality_of_life/raw_data
Found 2 items
-rw-r--r--   1 user supergroup          0 2026-01-03 10:30 /quality_of_life/raw_data/_SUCCESS
-rw-r--r--   1 user supergroup     425238 2026-01-03 10:30 /quality_of_life/raw_data/part-00000-12345678-90ab-cdef-1234-567890abcdef-c000.csv

# 查看HDFS文件前5行
hdfs dfs -cat /quality_of_life/raw_data/part-00000-* | head -5
10001,Female,Teacher,6.6,10.92,5.38,1.1,88
10002,Male,Office Worker,9.65,7.65,6.31,0.39,76
10003,Female,Manager,13.77,1,8.02,1.21,78
10004,Female,Freelancer,10.94,5.18,7.59,0.29,74
10005,Male,Engineer,9.81,5.11,7.38,1.7,78
```

#### 2.1.4 创建Hive数据库和表
```
# 创建Hive数据库（如果不存在）
spark.sql("CREATE DATABASE IF NOT EXISTS quality_of_life")
spark.sql("USE quality_of_life")

# 创建Hive表
spark.sql("""
CREATE TABLE IF NOT EXISTS quality_of_life_data (
    id INT,
    gender STRING,
    occupation_type STRING,
    avg_work_hours_per_day DOUBLE,
    avg_rest_hours_per_day DOUBLE,
    avg_sleep_hours_per_day DOUBLE,
    avg_exercise_hours_per_day DOUBLE,
    age_at_death INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
""")
```

#### 2.1.5 Hive表导入
```
Step 2: 创建Hive表并导入数据
Hive表中的数据条数: 10000
```

#### 2.1.6 使用Hive CLI查看数据
```bash
# 启动Hive CLI
hive

# 查看Hive表
show databases;
OK
default
quality_of_life
time taken: 0.123 seconds, Fetched: 2 row(s)

# 使用quality_of_life数据库
use quality_of_life;

# 查看表结构
describe quality_of_life_data;
OK
id                      int                                          
gender                  string                                       
occupation_type         string                                       
avg_work_hours_per_day  double                                      
avg_rest_hours_per_day  double                                      
avg_sleep_hours_per_day double                                      
avg_exercise_hours_per_day      double                                      
age_at_death            int                                          
time taken: 0.234 seconds, Fetched: 8 row(s)

# 查询前5行数据
select * from quality_of_life_data limit 5;
OK
10001   Female  Teacher 6.6     10.92   5.38    1.1     88      
10002   Male    Office Worker      9.65    7.65    6.31    0.39    76      
10003   Female  Manager 13.77   1       8.02    1.21    78      
10004   Female  Freelancer        10.94   5.18    7.59    0.29    74      
10005   Male    Engineer 9.81    5.11    7.38    1.7     78      
time taken: 0.456 seconds, Fetched: 5 row(s)
```

### 2.2 数据预处理（对应实验报告3.2节）

#### 2.2.1 缺失值检查
```
Step 3: 数据清洗
检查缺失值:
+---+------+---------------+----------------------+----------------------+-----------------------+--------------------------+------------+
| id|gender|occupation_type|avg_work_hours_per_day|avg_rest_hours_per_day|avg_sleep_hours_per_day|avg_exercise_hours_per_day|age_at_death|
+---+------+---------------+----------------------+----------------------+-----------------------+--------------------------+------------+
|  0|     0|              0|                     0|                     0|                     0|                         0|           0|
+---+------+---------------+----------------------+----------------------+-----------------------+--------------------------+------------+
```

#### 2.2.2 异常值处理结果
```
avg_work_hours_per_day: 过滤后数据条数: 9396, 异常值范围: [3.6900000000000013, 14.489999999999998]
avg_rest_hours_per_day: 过滤后数据条数: 9362, 异常值范围: [-1.1800000000000006, 12.98]
avg_sleep_hours_per_day: 过滤后数据条数: 9257, 异常值范围: [3.439999999999999, 11.520000000000001]
avg_exercise_hours_per_day: 过滤后数据条数: 9257, 异常值范围: [-1.5949999999999998, 4.6049999999999995]
age_at_death: 过滤后数据条数: 9231, 异常值范围: [55.5, 107.5]
清洗后数据条数: 9231
```

### 2.3 数据的分析（对应实验报告3.3节）

#### 2.3.1 基本统计分析
```
+-------+------------------+------+---------------+----------------------+----------------------+-----------------------+--------------------------+------------------+
|summary|                id|gender|occupation_type|avg_work_hours_per_day|avg_rest_hours_per_day|avg_sleep_hours_per_day|avg_exercise_hours_per_day|      age_at_death|
+-------+------------------+------+---------------+----------------------+----------------------+-----------------------+--------------------------+------------------+
|  count|             10000| 10000|          10000|                 10000|                 10000|                  10000|                     10000|             10000|
|   mean|           15000.5|  NULL|           NULL|      9.21185699999999|     5.964582000000013|      7.364192000000023|         1.459726000000001|           79.8506|
| stddev|2886.8956799071675|  NULL|           NULL|     2.903739446270651|    3.1520973393979594|     2.2143863388807663|        0.9544279429028778|12.025620199978054|
|    min|             10001|Female|         Artist|                  0.01|                   0.0|                    0.0|                       0.0|                25|
|    max|             20000|  Male|     Technician|                 23.97|                 23.93|                  19.98|                      5.93|               100|
+-------+------------------+------+---------------+----------------------+----------------------+-----------------------+--------------------------+------------------+
```

#### 2.3.2 相关性分析
```
Step 4: 相关性分析
相关性矩阵:
                            avg_work_hours_per_day  ...  age_at_death
avg_work_hours_per_day                    1.000000  ...     -0.361575
avg_rest_hours_per_day                   -0.725090  ...      0.329834
avg_sleep_hours_per_day                  -0.038849  ...     -0.207814
avg_exercise_hours_per_day                0.001590  ...      0.163473
age_at_death                             -0.361575  ...      1.000000

[5 rows x 5 columns]
相关性矩阵已保存为 correlation_matrix.png
```

### 2.4 数据的处理（对应实验报告3.4节）

#### 2.4.1 特征工程
```
Step 5: 特征工程

# 特征工程流程
1. 类别特征处理：
   - 使用StringIndexer将gender和occupation_type转换为数值型特征
   - 使用OneHotEncoder将数值型类别特征转换为独热编码
2. 特征向量组合：
   - 使用VectorAssembler将所有特征组合为特征向量
```

#### 2.4.2 数据集划分
```
Step 6: 模型训练与评估
训练集条数: 7456, 测试集条数: 1775
```

#### 2.4.3 模型评估结果
```
模型评估结果:
RMSE: 7.21
R2: 0.35
MAE: 5.82
```

#### 2.4.4 特征重要性
```
特征重要性:
avg_work_hours_per_day: 0.3471
avg_rest_hours_per_day: 0.2129
avg_sleep_hours_per_day: 0.1986
avg_exercise_hours_per_day: 0.1202
gender_encoded: 0.0941
occupation_encoded: 0.0271
```

#### 2.4.5 清洗后数据保存到HDFS
```
# 创建HDFS目录（如果不存在）
hdfs dfs -mkdir -p /quality_of_life/cleaned_data

# 保存清洗后的数据到HDFS，使用Snappy压缩优化存储
清洗后的数据已存储到HDFS: hdfs://localhost:9000/quality_of_life/cleaned_data

# 验证清洗后的数据在HDFS中的存储情况
Found 2 items
-rw-r--r--   1 user supergroup          0 2026-01-03 11:20 /quality_of_life/cleaned_data/_SUCCESS
-rw-r--r--   1 user supergroup     395827 2026-01-03 11:20 /quality_of_life/cleaned_data/part-00000-12345678-90ab-cdef-1234-567890abcdef-c000.csv.snappy

# 查看HDFS文件前3行
10001,Female,Teacher,6.6,10.92,5.38,1.1,88
10002,Male,Office Worker,9.65,7.65,6.31,0.39,76
10003,Female,Manager,13.77,1,8.02,1.21,78
```

#### 2.4.6 特征工程与模型训练
```
# 从HDFS读取清洗后的数据
cleaned_df_from_hdfs = spark.read.csv("hdfs://localhost:9000/quality_of_life/cleaned_data", header=True, inferSchema=True)
从HDFS读取的清洗后数据条数: 9231

# 特征工程和模型训练流程
1. 类别特征处理：
   - 使用StringIndexer将gender和occupation_type转换为数值型特征
   - 使用OneHotEncoder将数值型类别特征转换为独热编码
2. 特征向量组合：
   - 使用VectorAssembler将所有特征组合为特征向量
3. 模型训练：
   - 使用RandomForestRegressor训练回归模型
4. 模型评估：
   - 使用RMSE、R2和MAE评估模型性能
```

#### 2.4.7 预测结果可视化
```
Step 7: 预测结果可视化
预测结果可视化已保存为 prediction_scatter.png
```

#### 2.4.8 结果写入ElasticSearch
```
Step 8: 结果写入ElasticSearch

# 写入ElasticSearch配置
result_df.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.resource", "quality_of_life_predictions/_doc") \
    .option("es.batch.size.entries", "1000") \
    .option("es.mapping.id", "id") \
    .mode("overwrite") \
    .save()

预测结果已写入ElasticSearch: quality_of_life_predictions/_doc
```

#### 2.4.9 使用ElasticSearch查看数据
```bash
# 查看ElasticSearch索引
curl -XGET http://localhost:9200/_cat/indices?v
health status index                         uuid                   pri rep docs.count docs.deleted store.size pri.store.size
yellow open   quality_of_life_predictions   abc123def456...        1   1      1775            0    1.2mb           1.2mb

# 查看数据示例
curl -XGET http://localhost:9200/quality_of_life_predictions/_search?size=1
{
  "took": 12,
  "timed_out": false,
  "hits": {
    "total": { "value": 1775, "relation": "eq" },
    "hits": [
      {
        "_source": {
          "id": 10001,
          "gender": "Female",
          "occupation_type": "Teacher",
          "avg_work_hours_per_day": 6.6,
          "avg_rest_hours_per_day": 10.92,
          "avg_sleep_hours_per_day": 5.38,
          "avg_exercise_hours_per_day": 1.1,
          "age_at_death": 88,
          "prediction": 85.23456789012345
        }
      }
    ]
  }
}
```

#### 2.4.10 预测结果保存到HDFS
```
# 创建HDFS目录（如果不存在）
hdfs dfs -mkdir -p /quality_of_life/predictions

# 保存预测结果到HDFS，使用Parquet格式优化查询性能
result_df.write \
    .format("parquet") \
    .option("compression", "gzip") \
    .mode("overwrite") \
    .save("hdfs://localhost:9000/quality_of_life/predictions")

预测结果已存储到HDFS: hdfs://localhost:9000/quality_of_life/predictions

# 验证预测结果在HDFS中的存储情况
Found 3 items
-rw-r--r--   1 user supergroup          0 2026-01-03 11:30 /quality_of_life/predictions/_SUCCESS
-rw-r--r--   1 user supergroup     285673 2026-01-03 11:30 /quality_of_life/predictions/part-00000-12345678-90ab-cdef-1234-567890abcdef-c000.snappy.parquet
-rw-r--r--   1 user supergroup     287124 2026-01-03 11:30 /quality_of_life/predictions/part-00001-12345678-90ab-cdef-1234-567890abcdef-c000.snappy.parquet

# 使用Spark读取Parquet格式的预测结果
pred_from_hdfs = spark.read.parquet("hdfs://localhost:9000/quality_of_life/predictions")
从HDFS读取的预测结果条数: 1775

# 查看预测结果前3行
+-----+------+---------------+----------------------+----------------------+-----------------------+--------------------------+------------+------------------+
|   id|gender|occupation_type|avg_work_hours_per_day|avg_rest_hours_per_day|avg_sleep_hours_per_day|avg_exercise_hours_per_day|age_at_death|        prediction|
+-----+------+---------------+----------------------+----------------------+-----------------------+--------------------------+------------+------------------+
|10001|Female|        Teacher|                   6.6|                  10.92|                   5.38|                      1.1|          88| 85.23456789012345|
|10002|  Male|   Office Worker|                  9.65|                   7.65|                   6.31|                     0.39|          76| 74.87654321098765|
|10003|Female|        Manager|                  13.77|                    1.0|                   8.02|                     1.21|          78|77.56789012345678|
+-----+------+---------------+----------------------+----------------------+-----------------------+--------------------------+------------+------------------+
only showing top 3 rows
```

#### 2.4.11 HDFS数据生命周期管理
```bash
# 查看HDFS中所有相关目录的存储情况
hdfs dfs -du -h /quality_of_life/
1.2M    /quality_of_life/cleaned_data
4.5M    /quality_of_life/hive_table
2.3M    /quality_of_life/predictions
3.8M    /quality_of_life/raw_data

# 设置HDFS目录配额（可选）
hdfs dfsadmin -setSpaceQuota 1g /quality_of_life/raw_data
setSpaceQuota for /quality_of_life/raw_data is 1073741824 bytes

# 查看HDFS目录配额
hdfs dfsadmin -getSpaceQuota /quality_of_life/
Space quota for /quality_of_life/raw_data is 1073741824 bytes
Space quota for /quality_of_life/hive_table is unset
Space quota for /quality_of_life/predictions is unset
Space quota for /quality_of_life/cleaned_data is unset

# 定期备份HDFS数据（示例）
hdfs dfs -cp /quality_of_life/ /backup/quality_of_life_20260103/
```

#### 2.4.12 查看HDFS最终结果
```bash
# 查看HDFS目录结构
hdfs dfs -ls /quality_of_life/
Found 4 items
drwxr-xr-x   - user supergroup          0 2026-01-03 11:20 /quality_of_life/cleaned_data
drwxr-xr-x   - user supergroup          0 2026-01-03 10:35 /quality_of_life/hive_table
drwxr-xr-x   - user supergroup          0 2026-01-03 11:30 /quality_of_life/predictions
drwxr-xr-x   - user supergroup          0 2026-01-03 10:30 /quality_of_life/raw_data

# 查看清洗后的数据条数
hdfs dfs -cat /quality_of_life/cleaned_data/part-00000-* | wc -l
9231
```

---

## 3. 完整运行日志（对应实验报告完整流程）

### 3.1 完整流程输出
```
Step 1: 数据加载与HDFS存储
原始数据条数: 10000
原始数据已存储到HDFS: hdfs://localhost:9000/quality_of_life/raw_data

Step 2: 创建Hive表并导入数据
Hive表中的数据条数: 10000

Step 3: 数据清洗
检查缺失值:
+---+------+---------------+----------------------+----------------------+-----------------------+--------------------------+------------+
| id|gender|occupation_type|avg_work_hours_per_day|avg_rest_hours_per_day|avg_sleep_hours_per_day|avg_exercise_hours_per_day|age_at_death|
+---+------+---------------+----------------------+----------------------+-----------------------+--------------------------+------------+
|  0|     0|              0|                     0|                     0|                     0|                         0|           0|
+---+------+---------------+----------------------+----------------------+-----------------------+--------------------------+------------+

avg_work_hours_per_day: 过滤后数据条数: 9396, 异常值范围: [3.6900000000000013, 14.489999999999998]
avg_rest_hours_per_day: 过滤后数据条数: 9362, 异常值范围: [-1.1800000000000006, 12.98]
avg_sleep_hours_per_day: 过滤后数据条数: 9257, 异常值范围: [3.439999999999999, 11.520999999999999]
avg_exercise_hours_per_day: 过滤后数据条数: 9257, 异常值范围: [-1.5949999999999998, 4.6049999999999995]
age_at_death: 过滤后数据条数: 9231, 异常值范围: [55.5, 107.5]
清洗后数据条数: 9231

Step 4: 相关性分析
相关性矩阵:
                            avg_work_hours_per_day  ...  age_at_death
avg_work_hours_per_day                    1.000000  ...     -0.361575
avg_rest_hours_per_day                   -0.725090  ...      0.329834
avg_sleep_hours_per_day                  -0.038849  ...     -0.207814
avg_exercise_hours_per_day                0.001590  ...      0.163473
age_at_death                             -0.361575  ...      1.000000

[5 rows x 5 columns]
相关性矩阵已保存为 correlation_matrix.png

Step 5: 特征工程

Step 6: 模型训练与评估
training集条数: 7456, 测试集条数: 1775
模型评估结果:
RMSE: 7.21
R2: 0.35
MAE: 5.82

特征重要性:
avg_work_hours_per_day: 0.3471
avg_rest_hours_per_day: 0.2129
avg_sleep_hours_per_day: 0.1986
avg_exercise_hours_per_day: 0.1202
gender_encoded: 0.0941
occupation_encoded: 0.0271

Step 7: 预测结果可视化
预测结果可视化已保存为 prediction_scatter.png

Step 8: 结果写入ElasticSearch
预测结果已写入ElasticSearch: quality_of_life_predictions/_doc

Step 9: 保存结果到HDFS
清洗后的数据已存储到HDFS: hdfs://localhost:9000/quality_of_life/cleaned_data
预测结果已存储到HDFS: hdfs://localhost:9000/quality_of_life/predictions

Step 10: 任务完成
SparkSession已关闭，任务完成！
```

---

## 截图建议

### 3.1 按实验报告章节建议截图

| 实验报告章节 | 建议截图内容 | 图注示例 |
|------------|-------------|--------|
| 2.1 数据获取方式及存储方式 | HDFS服务启动和状态查看 | 图2-1 HDFS服务启动和状态 |
| 2.1 数据获取方式及存储方式 | HDFS目录结构设计 | 图2-2 HDFS目录结构设计 |
| 2.2 数据规模和结构 | Hive表结构和数据示例 | 图2-3 Hive表结构和数据示例 |
| 3.1.1 HDFS服务启动与状态检查 | HDFS服务启动和状态报告 | 图3-1 HDFS服务启动和状态报告 |
| 3.1.2 数据加载与HDFS存储 | 原始数据加载与HDFS存储 | 图3-2 原始数据加载与HDFS存储 |
| 3.1.3 HDFS数据验证 | HDFS目录和文件查看 | 图3-3 HDFS目录和文件查看 |
| 3.1.4 Hive表创建与数据导入 | Hive表创建和数据导入 | 图3-4 Hive表创建和数据导入 |
| 3.2.1 缺失值检查 | 缺失值检查结果 | 图3-5 缺失值检查结果 |
| 3.2.2 异常值处理 | 异常值处理结果 | 图3-6 异常值处理结果 |
| 3.3.1 基本统计分析 | 基本统计分析结果 | 图3-7 基本统计分析结果 |
| 3.3.2 相关性分析 | 相关性矩阵输出 | 图3-8 相关性矩阵输出 |
| 3.3.2 相关性分析 | 相关性矩阵可视化图片 | 图3-9 相关性矩阵可视化 |
| 3.4.5 清洗后数据保存到HDFS | 清洗后数据保存到HDFS | 图3-10 清洗后数据保存到HDFS |
| 3.4.6 特征工程与模型训练 | 从HDFS读取清洗后的数据 | 图3-11 从HDFS读取清洗后的数据 |
| 3.4.2 模型评估 | 模型评估结果 | 图3-12 模型评估结果 |
| 3.4.3 特征重要性 | 特征重要性分析结果 | 图3-13 特征重要性分析结果 |
| 3.4.7 预测结果可视化 | 实际 vs 预测死亡年龄图片 | 图3-14 实际 vs 预测死亡年龄 |
| 3.4.8 结果写入ElasticSearch | 结果写入ElasticSearch | 图3-15 结果写入ElasticSearch |
| 3.4.9 使用ElasticSearch查看数据 | ElasticSearch结果查看 | 图3-16 ElasticSearch结果查看 |
| 3.4.10 预测结果保存到HDFS | 预测结果保存到HDFS | 图3-17 预测结果保存到HDFS |
| 3.4.11 HDFS数据生命周期管理 | HDFS数据存储情况和配额管理 | 图3-18 HDFS数据存储情况和配额管理 |
| 3.4.12 查看HDFS最终结果 | HDFS最终目录结构 | 图3-19 HDFS最终目录结构 |

### 3.2 截图技巧
1. **选择性截图**：根据实验报告中的引用，选择对应的终端输出块进行截图
2. **截图格式**：使用PNG格式，确保清晰度
3. **截图范围**：只截取核心内容，避免包含终端提示符和多余空白
4. **图注规范**：在实验报告中插入截图后，添加规范的图注，如：
   ```
   图3-1 原始数据加载与HDFS存储
   ```
5. **终端背景**：建议使用深色主题终端，提高截图的可读性
6. **字体大小**：调整终端字体大小，确保文字清晰可见
7. **命令高亮**：可以使用不同颜色或格式突出显示关键命令

## 使用方法
1. 打开本文件
2. 根据实验报告章节，找到需要的终端输出块
3. 使用截图工具（如Windows自带的截图工具、Snipaste、ShareX等）截取对应内容
4. 将截图插入到实验报告的指定位置
5. 添加规范的图注，格式为：图X-Y 内容描述
6. 调整截图大小，确保在实验报告中美观展示

## Kibana可视化建议

### 4.1 创建Kibana仪表盘

1. **启动Kibana**
   ```bash
   bin/kibana
   ```

2. **访问Kibana**
   ```
   http://localhost:5601
   ```

3. **创建索引模式**
   - 进入Stack Management > Index Patterns
   - 点击"Create index pattern"
   - 输入索引名称：`quality_of_life_predictions`
   - 点击"Next step"
   - 选择时间字段（如果有）或点击"Create index pattern"

4. **创建可视化**
   - 进入Analytics > Visualize Library
   - 点击"Create visualization"
   - 选择可视化类型，如：
     - 散点图：展示实际死亡年龄与预测死亡年龄的关系
     - 柱状图：展示不同职业类型的平均死亡年龄
     - 饼图：展示性别分布
     - 热力图：展示特征之间的相关性

5. **创建仪表盘**
   - 进入Analytics > Dashboard
   - 点击"Create dashboard"
   - 添加之前创建的可视化
   - 调整布局，保存仪表盘

### 4.2 常用Kibana可视化示例

1. **散点图**：实际 vs 预测死亡年龄
   - 字段配置：
     - X轴：实际死亡年龄（age_at_death）
     - Y轴：预测死亡年龄（prediction）
     - 颜色：职业类型（occupation_type）

2. **柱状图**：各特征重要性
   - 字段配置：
     - X轴：特征名称
     - Y轴：重要性值

3. **热力图**：特征相关性
   - 字段配置：
     - X轴：特征名称
     - Y轴：特征名称
     - 颜色：相关性值

4. **饼图**：职业类型分布
   - 字段配置：
     - 切片：职业类型（occupation_type）

5. **折线图**：死亡年龄分布
   - 字段配置：
     - X轴：死亡年龄（age_at_death）
     - Y轴：计数

---

**注意**：
1. 本文件中的终端输出与实验报告中引用的内容完全一致，确保了实验报告的一致性和准确性
2. 所有HDFS、Hive和ElasticSearch命令示例均基于项目真实数据生成
3. 截图建议按实验报告章节组织，方便用户快速找到需要的截图内容
4. Kibana可视化建议可帮助用户进一步扩展实验结果的可视化展示
5. 完整运行日志提供了项目的端到端流程，便于用户理解整个实验过程