# 📊 生活质量 Spark 分析 | Quality of Life Spark Analytics

> **基于 Apache Spark 的生活质量数据分析系统——死亡率年龄预测、区域生活质量评估、分布式大数据处理。**
>
> *Quality of life data analysis system based on Apache Spark — mortality age prediction, regional quality of life assessment, distributed big data processing.*

---

## ⭐ 核心卖点 | Why Star This

| 卖点 | Feature | 一句话 |
|------|---------|--------|
| 🐘 **Spark 处理** | Spark Processing | 分布式大数据处理框架 |
| 📈 **死亡年龄预测** | Mortality Prediction | 基于机器学习的生活质量预测 |
| 🌍 **区域评估** | Regional Assessment | 多区域生活质量对比分析 |
| 📊 **可视化** | Visualization | 生活质量指标可视化展示 |
| 🔍 **多维度分析** | Multi-Dimension | 健康、经济、教育、环境多维指标 |

---

## 🏆 技术栈 | Tech Stack

![Python](https://img.shields.io/badge/Python-3.8+-blue?logo=python)
![Apache Spark](https://img.shields.io/badge/Spark-3.0+-orange?logo=apachespark)
![PySpark](https://img.shields.io/badge/PySpark-3.0+-orange?logo=apachespark)
![Pandas](https://img.shields.io/badge/Pandas-1.3+-blue?logo=pandas)
![Matplotlib](https://img.shields.io/badge/Matplotlib-3.4+-red?logo=matplotlib)

---

## 🚀 快速开始 | Quick Start

```bash
git clone https://github.com/Windyhhh/Quality-of-Life-Spark-Analytics.git
cd Quality-of-Life-Spark-Analytics

# 1. 安装依赖
pip install -r requirements.txt

# 2. 数据预处理
python src/preprocess.py --input data/raw.csv --output data/processed/

# 3. Spark 数据分析
spark-submit src/spark_analysis.py --data data/processed/

# 4. 训练预测模型
python src/train_model.py --data data/processed/train.csv

# 5. 生成可视化报告
python src/generate_report.py
```

---

## 📂 项目结构 | Project Structure

```
Quality-of-Life-Spark-Analytics/
├── src/                       # 核心代码
│   ├── preprocess.py          # 数据预处理
│   ├── spark_analysis.py      # Spark 分析
│   ├── train_model.py         # 模型训练
│   ├── mortality_prediction.py # 死亡年龄预测
│   └── generate_report.py     # 报告生成
├── data/                      # 数据
├── result/                    # 结果
└── requirements.txt
```

---

## 🔬 核心实现 | Core Implementation

### Spark 生活质量分析 | Spark QoL Analysis

```python
# PySpark 生活质量数据分析
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, desc

def analyze_quality_of_life(data_path):
    """Spark 分布式生活质量分析"""
    spark = SparkSession.builder \
        .appName("QoL-Analysis") \
        .getOrCreate()
    
    # 1. 加载数据
    df = spark.read.csv(data_path, header=True, inferSchema=True)
    
    # 2. 区域平均生活质量
    region_qol = df.groupBy("region") \
        .agg(avg("health_score").alias("avg_health"),
             avg("income").alias("avg_income"),
             avg("education").alias("avg_education")) \
        .orderBy(desc("avg_health"))
    
    # 3. 综合生活质量指数
    from pyspark.sql.functions import expr
    df = df.withColumn("qol_index", 
        expr("health_score*0.3 + income*0.3 + education*0.2 + environment*0.2"))
    
    # 4. 死亡年龄预测特征
    features = df.select("age", "health_score", "income", 
                         "education", "environment", "disease_rate")
    
    return region_qol, features
```

---

## 🎯 应用场景 | Use Cases

- 🏛️ **政府决策**：区域生活质量评估
- 📊 **公共健康**：死亡率与健康因素分析
- 🏙️ **城市规划**：城市宜居性研究
- 🎓 **大数据教学**：Spark 数据分析项目
- 📈 **社会研究**：生活质量影响因素研究

---

## 📄 License

MIT License — 自由使用、修改和分发。

---

> 💡 **Spark 生活质量大数据分析，Star ⭐ 洞察民生数据！**
