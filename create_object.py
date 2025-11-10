import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import datetime
import duckdb

# -----------------------------------------
#              データ読み込み
# -----------------------------------------
def load_data():
    df = pd.read_csv('data/sample_orders.csv')
    df['order_at'] = pd.to_datetime(df['order_at'])
    # master = pd.read_csv('data/traffic_master.csv')

    return df

# -----------------------------------------
#              分析カラム追加
# -----------------------------------------
def add_columns(_df: pd.DataFrame) -> pd.DataFrame:
  """
  DuckDBで集計・加工を行い、分析用カラムを追加する関数
  """
  
  # Duckdbセッションを作成
  con = duckdb.connect(database=':memory:')

  # pandas DataFrameをDuckDBテーブルとして登録
  con.register('raw', _df)

  # SQLで追加カラムを作成
  query = """
    WITH base AS (
      SELECT *,
        /* 流入元分類*/
        CASE
          WHEN purchase_url LIKE 'ad_%' THEN 'AD'
          WHEN purchase_url LIKE 'shop_%' THEN 'shop'
          WHEN purchase_url LIKE 'list_%' THEN 'listing'
          WHEN purchase_url LIKE 'ins_%' THEN 'instagram'
          WHEN purchase_url LIKE 'tik_%' THEN 'tiktok'
          WHEN purchase_url LIKE 'line%' THEN 'LINE'
          WHEN purchase_url LIKE 'rp_dm%' THEN 'DM'
          WHEN purchase_url LIKE 'rp_outb%' THEN 'outbound'
          WHEN purchase_url LIKE 'rp_mg' THEN 'mg'
          ELSE 'other'
        END AS traffic_source
      FROM raw
    ),
    with_counts AS (
      SELECT
        b.*,
        COUNT(*) OVER (PARTITION BY customer_num ORDER BY order_at) AS purchase_count,
        MIN(order_at) OVER (PARTITION BY customer_num) AS first_order_date
      FROM base b
    )
    SELECT
      *,
      CASE WHEN order_at = first_order_date THEN 1 ELSE 0 END AS first_flag,
      strftime(order_at, '%Y-%m') AS month,
      strftime(order_at, '%Y') AS year,
      CASE
        WHEN paid_price < 3000 THEN 'low'
        WHEN paid_price BETWEEN 3000 AND 6000 THEN 'mid'
        ELSE 'high'
      END AS price_segment
    FROM with_counts
  """

  enriched_df = con.execute(query).df()
  con.close()

  return enriched_df

# -----------------------------------------
#              SQL実行関数
# -----------------------------------------
def filter_data(mode, df, start_date=None, end_date=None, target_month=None):
    """
    Streamlitから呼び出されるデータフィルタ関数
    mode: '比較' or 'スポット'
    """

    # スポットの場合のみ、target_monthから期間を算出
    if mode == 'スポット' and target_month is not None:
        dt = datetime.strptime(target_month, '%Y-%m')
        start_date = dt.strftime('%Y-%m-01')
        # 月末を算出(どの月にも存在する28日に4日足して翌月->1日を算出してから1日引く)
        end_date = ((dt.replace(day=28) + pd.Timedelta(days=4)).replace(day=1) - pd.Timedelta(days=1)).strftime('%Y-%m-%d')

    # Duckdbセッションを作成
    con = duckdb.connect(database=':memory:')
    con.register('raw', df)

    # SQLは共通化
    query = f"""
        SELECT *
        FROM raw
        WHERE order_at BETWEEN '{start_date}' AND '{end_date}' 
    """

    # 実行
    df_filtered = con.execute(query).df()
    con.close()

    return df_filtered


# -----------------------------------------
#              比較モード選択時
# -----------------------------------------

# ============= ① 流入元分布 =============
# 新規, リピート流入共通関数
def plot_flow(df, kind="new"):
    """
    kind: "new"（新規）または "repeat"（リピート）
    """

    # フィルタ設定
    if kind == "new":
        flag = 1
        title = "月別新規ユーザー流入推移"
    else:
        flag = 0
        title = "月別リピートユーザー流入推移"

    # フィルタ適用
    df_filtered = df[df["first_flag"] == flag]

    # 各月×流入元の件数を集計
    monthly_counts = (
        df_filtered.groupby(["month", "traffic_source"])
        .size()
        .reset_index(name="count")
    )

    # 月ごとの合計を計算して割合化
    monthly_counts["total"] = monthly_counts.groupby("month")["count"].transform("sum")
    monthly_counts["share"] = monthly_counts["count"] / monthly_counts["total"] * 100

    # 折れ線グラフ
    fig = px.line(
        monthly_counts,
        x="month",
        y="share",
        color="traffic_source",
        markers=True,
        title=title,
        color_discrete_sequence=px.colors.qualitative.Set2,
    )

    # レイアウト調整
    fig.update_layout(
        yaxis_title="構成比（%）",
        xaxis_title="注文月",
        legend_title="流入元",
        hovermode="x unified",
        width=850,
        height=500,
        margin=dict(l=40, r=40, t=80, b=40)
    )

    return fig

# ============= ② リピート率 =============
# リピート件数とリピート率
def plot_repeat_rate(df: pd.DataFrame):
  """
  月別のリピート件数（棒）+ リピート率（折れ線）を描画する関数
  """

  # 月*first_flagで集計
  df_group = df.groupby(['month', 'first_flag']).size().unstack(fill_value=0)

  # 集計値の整形
  df_group['total'] = df_group.sum(axis=1)
  df_group['repeat_rate'] = (df_group[0] / df_group['total']) * 100
  df_group = df_group.reset_index()

  # グラフ生成
  fig = go.Figure()

  # 棒グラフ（リピート件数）
  fig.add_bar(
      x=df_group['month'],
      y=df_group[0],
      name='リピート件数',
      marker_color='#6BAED6',
      yaxis='y1'
  )

  # 折れ線グラフ（リピート率）
  fig.add_trace(go.Scatter(
      x=df_group['month'],
      y=df_group['repeat_rate'],
      mode='lines+markers',
      name='リピート率 (%)',
      line=dict(color='#FF7F0E', width=3),
      yaxis="y2"
  ))

  # レイアウト設定
  fig.update_layout(
      title='リピート率とリピート件数の推移',
      xaxis_title='注文月',
      yaxis=dict(
          title='件数',
          showgrid=False
      ),
      yaxis2=dict(
          title='リピート率 (%)',
          overlaying='y',
          side='right',
          showgrid=False
      ),
      hovermode='x unified',
      width=850,
      height=500,
      legend=dict(
          orientation='h',
          yanchor='bottom',
          y=1.02,
          xanchor='right',
          x=1
      ),
      margin=dict(l=40, r=60, t=80, b=40)
  )

  return fig

# -----------------------------------------
#             スポットモード選択時
# -----------------------------------------

# ============= ① 流入元分布 =============
def draw_spot_pie(df, kind):
    """
    新規/リピート流入の共通円グラフ描画関数
    kind: 'new' or 'repeat'
    """

    # フィルタ設定
    if kind == 'new':
        flag = 1
        title = '新規流入 構成比'
        color_palette = px.colors.qualitative.Set2
    elif kind == 'repeat':
        flag = 0
        title = 'リピート流入 構成比'
        color_palette = px.colors.qualitative.Pastel1
    else:
        raise ValueError('kind は "new" または "repeat" のみ指定可能です。')
    
    # フィルタ適用
    df_filtered = df[df['first_flag'] == flag]

    # グラフ描画
    fig = px.pie(
        df_filtered,
        names='traffic_source',
        title=title,
        hole=0.4,
        color_discrete_sequence=color_palette
    )

    fig.update_traces(
        textinfo='percent+label',
        hovertemplate='%{label}<br>件数: %{value}<br>構成比: %{percent}'
    )

    return fig

# ============= ② リピート率 =============
# リピート件数とリピート率
def draw_spot_repeat_rate(df: pd.DataFrame):
    """
    スポットモード用リピート率円グラフ描画関数
    """

    df_group = df.groupby('first_flag').size().reset_index(name='count')
    fig_repeat = px.pie(
        df_group,
        names='first_flag',
        values='count',
        title='リピート率(新規 vs リピート)',
        hole=0.4,
        color_discrete_map={1: '#66b3ff', 0: '#ff9999'},
    )
    fig_repeat.update_traces(
        textinfo='percent+label',
        hovertemplate='%{label}<br>件数: %{value}<br>構成比: %{percent}'
    )
    fig_repeat.update_layout(showlegend=True)

    return fig_repeat

# ============= ③ 流入別リピート率 =============
# 流入チャネル別リピート率(棒グラフ+折れ線グラフ)
def draw_spot_flow_repeat(df: pd.DataFrame):
    """
    スポットモード用リピート率棒グラフ+折れ線描画関数
    """

    df_rate = (
        df.groupby('traffic_source')['first_flag']
        .agg(total='count', repeat=lambda x: (x == 0).sum())
        .reset_index()
    )
    df_rate['repeat_rate'] = (df_rate['repeat'] / df_rate['total']) * 100

    fig = px.bar(df_rate, x='traffic_source', y='total', title='流入別 新規件数・リピート率')
    fig.add_scatter(x=df_rate['traffic_source'], y=df_rate['repeat_rate'], mode='lines+markers', name='リピート率(%)')

    return fig