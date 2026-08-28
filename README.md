<div align="center">

# 📊 Quality-of-Life-Spark-Analytics

### Spark-based mortality-age prediction.

Big-data + ML analytics over quality-of-life data, with correlation analysis, predictions and an HDFS variant.

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.8+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Spark-3-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)

</div>

---

**Quality-of-Life-Spark-Analytics** analyzes quality-of-life data with **Apache Spark** to predict **mortality age** — combining big-data processing, correlation analysis, prediction scatter plots and an **HDFS** deployment variant.

> [!NOTE]
> 中文项目：基于 Spark 的生活质量数据分析与死亡年龄预测——大数据处理 + 相关性分析 + 预测可视化，支持 HDFS 集群版。

---

## Features

- **Spark analytics** — distributed quality-of-life processing.
- **Mortality-age prediction** — scatter + correlation visualizations.
- **HDFS variant** — `quality_of_life_hdfs.py` for cluster runs.
- **End-to-end** — data → model → predictions CSV.

---

## Quickstart

```bash
git clone https://github.com/Windyhhh/Quality-of-Life-Spark-Analytics.git
cd Quality-of-Life-Spark-Analytics

pip install -r requirements.txt

python quality_of_life_analysis.py    # local analysis
python quality_of_life_hdfs.py        # HDFS-backed variant
```

Predictions land in `predictions_result/predictions.csv`.

---

## Project Structure

```
Quality-of-Life-Spark-Analytics/
├── quality_of_life_analysis.py
├── quality_of_life_hdfs.py
├── Updated Quality of Life Data.csv
├── predictions_result/predictions.csv
└── *.png                    # correlation / scatter figures
```

---

## License

MIT — free to use, modify and distribute.
