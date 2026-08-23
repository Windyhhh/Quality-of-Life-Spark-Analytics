# 📈 Quality of Life Spark Analytics | 生活质量数据分析（Spark 死亡年龄预测）

> **Big data analysis of quality of life factors and mortality age prediction using Apache Spark. Explore socioeconomic, healthcare, and lifestyle factors, build predictive models for life expectancy. Spark MLlib + visualization.**
>
> 基于 Apache Spark 的生活质量因素与死亡年龄预测大数据分析。探索社会经济、医疗和生活方式因素，构建预期寿命预测模型。Spark MLlib + 可视化。

---

## 🌟 Features | 核心特性

- **Spark MLlib** — Distributed machine learning
- **Life Expectancy Prediction** — Regression models for mortality age
- **Factor Analysis** — Correlation and importance of QoL factors
- **Big Data Processing** — Scalable Spark SQL/DataFrame
- **Visualization** — Charts and dashboards
- **Multiple Models** — Linear regression, random forest, GBT

---

## 🚀 Quick Start | 快速开始

```bash
# Submit Spark job
spark-submit quality_of_life_analysis.py --data quality_of_life.csv

# Train prediction model
spark-submit train_life_expectancy.py --model rf --output model/

# Generate visualization
python visualize_results.py --results results/
```

---

## 📊 Analysis Dimensions | 分析维度

| Category | Factors |
|----------|---------|
| **Economy** | GDP, income, poverty rate |
| **Healthcare** | Hospital beds, doctors, healthcare spending |
| **Lifestyle** | Smoking, alcohol, exercise, diet |
| **Education** | Literacy rate, school years |
| **Environment** | Air quality, water, sanitation |
| **Social** | Marriage rate, family size, social support |

---

## 📄 License | 许可证

MIT License.

[GitHub](https://github.com/Windyhhh/Quality-of-Life-Spark-Analytics)
