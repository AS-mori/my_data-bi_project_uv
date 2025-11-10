# データマーケ分析アプリ (Streamlit + uv)

## 🧭 概要
健康食品のサブスク受注データを分析し、流入元別に新規・リピート傾向を可視化。

## 🚀 実行環境
- Python 3.8.16 (uv仮想環境)
- Streamlit, Plotly, DuckDB, Pandas

## 🗂 構成
- `main.py` : Streamlit本体
- `create_object.py` : SQL・グラフ処理関数
- `data/` : サンプルデータ

## 🧩 実行方法
```bash
uv run streamlit run main.py
