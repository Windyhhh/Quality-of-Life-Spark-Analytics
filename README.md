<div align="center">

# 📊 Quality-of-Life-Spark-Analytics

### Spark mortality-age prediction system.

Spark-based analysis and prediction over quality-of-life data.

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.8+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Spark-3-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)

</div>

---

**Quality-of-Life-Spark-Analytics** analyzes quality-of-life data with **Apache Spark** and predicts **mortality age** — including correlation analysis, prediction scatter plots and an HDFS-backed variant.

> [!NOTE]
> 中文项目：生活质量数据分析——Spark 死亡率年龄预测系统。

---

## Quickstart

```bash
git clone https://github.com/Windyhhh/Quality-of-Life-Spark-Analytics.git
cd Quality-of-Life-Spark-Analytics

pip install -r requirements.txt

# run the analysis
python quality_of_life_analysis.py

# HDFS-backed variant
python quality_of_life_hdfs.py
```

Predictions land in `predictions_result/predictions.csv`.

---

## Features

- **Spark analytics** — distributed quality-of-life analysis.
- **Mortality-age prediction** — prediction scatter + correlations.
- **HDFS variant** — run on a Hadoop cluster.

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
