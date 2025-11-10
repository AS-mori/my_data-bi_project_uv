# ライブラリインポート
import streamlit as st
import pandas as pd
import datetime
import create_object as co
import duckdb

# -----------------------------------------
#          csvからデータを読み込み
# -----------------------------------------
_df = co.load_data()

# 分析時に必要なカラムが少ないため読み込み時に追加
df = co.add_columns(_df)

# -----------------------------------------
#       Streamlitのレイアウト - 全体 -
# -----------------------------------------
# 全体レイアウト
st.set_page_config(
    page_title='注文データ分析APP',
    layout='wide' 
)

# タイトル
st.title('📊 注文データ分析ダッシュボード')

# データ読み込み
# st.subheader('注文データ(先頭5件)')
# st.dataframe(df.head())

# st.subheader('流入マスタ')
# st.dataframe(master.head())

# -----------------------------------------
#     Streamlitのレイアウト - サイドバー -
# -----------------------------------------
# メニューの設定等に必要な一覧を取得
# products_list = df['product_code'].unique()
mode_list = ['比較', 'スポット']

st.sidebar.header('分析設定')

# モード選択
mode = st.sidebar.selectbox('分析タイプを選んでください', mode_list)

# 日付選択  
if mode == '比較':
    # データ読み込み時に最小と最大を取得して変数格納したい
    start_date = st.sidebar.date_input('開始日', datetime.date(2024, 9, 1))
    end_date = st.sidebar.date_input('終了日', datetime.date(2025, 8, 31))
else:
    target_month = st.sidebar.selectbox(
        '分析対象月を選択してください',
        [f'2024-{m:02d}' for m in range(9, 13)] + [f'2025-{m:02d}' for m in range(1, 9)]
        )

# 送信ボタン
submit_button = st.sidebar.button(label = '分析開始')

# サイドバーの設定
# with st.sidebar.form(key='my_form'):

    # 商品選択
    # product = st.multiselect('商品を選択してください', products_list)   # チェックボックスにしたい
    # if len(product) != 0:
    #     product = '","'.join(product)
    #     country = f'Product in ("{product}")'
    # else:
    #     product = 'True'

    # モード選択
    # mode = st.selectbox('分析タイプを選んでください', mode_list)

    # 日付選択
    # if mode == '比較':
    #     # データ読み込み時に最小と最大を取得して変数格納したい
    #     start_date = st.date_input('開始日', datetime.date(2024, 9, 1))
    #     end_date = st.date_input('終了日', datetime.date(2025, 8, 31))
    # else:
    #     target_date = st.date_input('分析日', datetime.date(2024, 9, 1))
    
    # 送信ボタン
    # submit_button = st.form_submit_button(label = '分析開始')

# -----------------------------------------
#           分析実行・グラフ化・表示
# -----------------------------------------
if submit_button:
    ###### 分析 ######
    # st.success(f'{mode}モードでデータを分析中...')
    if mode == '比較':
        df_filtered = co.filter_data(mode, df, start_date=start_date, end_date=end_date)
    else:
        df_filtered = co.filter_data(mode, df, target_month=target_month)

    ###### グラフ表示 ######
    # グラフの描写
    if mode == '比較':
        try:
            fig_new = co.plot_flow(df_filtered, kind='new')
            st.plotly_chart(fig_new, use_container_width=True)

            fig_repeat_rate = co.plot_repeat_rate(df_filtered)
            st.plotly_chart(fig_repeat_rate, use_container_width=True)

            fig_repeat = co.plot_flow(df_filtered, kind='repeat')
            st.plotly_chart(fig_repeat, use_container_width=True)
        except:
            print('グラフ作成に失敗しました')
    else:
        try:
            # 横並びにする列を定義
            col1, col2 = st.columns(2)
            col3, col4 = st.columns(2)

            # 上段左カラム
            with col1:
                fig_new = co.draw_spot_pie(df_filtered, kind='new')
                st.plotly_chart(fig_new, use_container_width=True)

            # 上段右カラム
            with col2:
                fig_repeat = co.draw_spot_pie(df_filtered, kind='repeat')
                st.plotly_chart(fig_repeat, use_container_width=True)

            # 下段左カラム
            with col3:
                fig_repeat_rate = co.draw_spot_repeat_rate(df_filtered)
                st.plotly_chart(fig_repeat_rate, use_container_width=True)
            
            # 下段右カラム
            with col4:
                fig_flow_repeat = co.draw_spot_flow_repeat(df_filtered)
                st.plotly_chart(fig_flow_repeat, use_container_width=True)
        except:
            print('グラフ作成に失敗しました')
    
    # csvとしてダウンロードするボタン
    st.download_button(
        'Press to Download',
        df_filtered.to_csv(index=False).encode('utf-8-sig'),
        'file.csv',
        'text/csv',
        key='download-csv'
    )

    # データの表示(先頭100件)
    st.table(df_filtered.head(100))
else:
    st.info('条件を指定してサイドバーの「分析開始」ボタンを押してください')