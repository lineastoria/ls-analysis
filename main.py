import pandas as pd
import psycopg2
import sshtunnel
from psycopg2.extras import DictCursor
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import requests
from typing import Dict, List, Tuple, Any, Optional
import tempfile
import os
import stat
import streamlit as st
import numpy as np


class ShopifyAnalyzer:
    def __init__(
        self,
        db_config: Dict[str, Any],
        shopify_config: Dict[str, Any],
        ssh_key_content: str,
    ):
        self.db_config = db_config
        self.shopify_config = shopify_config
        self.ssh_key_content = ssh_key_content
        self.connection = None
        self.tunnel = None
        self._temp_key_file = None

    def create_temp_key_file(self) -> str:
        try:
            self._temp_key_file = tempfile.NamedTemporaryFile(
                mode="w", delete=False, prefix="ssh_key_", suffix=".pem"
            )
            self._temp_key_file.write(self.ssh_key_content)
            self._temp_key_file.flush()
            self._temp_key_file.close()
            os.chmod(self._temp_key_file.name, stat.S_IRUSR | stat.S_IWUSR)
            return self._temp_key_file.name
        except Exception as e:
            if self._temp_key_file and os.path.exists(self._temp_key_file.name):
                os.unlink(self._temp_key_file.name)
            raise Exception(f"Failed to create SSH key file: {str(e)}")

    def connect_db(
        self,
    ) -> Tuple[psycopg2.extensions.connection, sshtunnel.SSHTunnelForwarder]:
        try:
            key_file_path = self.create_temp_key_file()
            
            with st.spinner('データベースに接続中...'):
                self.tunnel = sshtunnel.SSHTunnelForwarder(
                    (self.db_config["bastion_host"]),
                    ssh_username=self.db_config["bastion_user"],
                    ssh_pkey=key_file_path,
                    ssh_private_key_password=None,
                    allow_agent=False,
                    host_pkey_directories=[],
                    remote_bind_address=(
                        self.db_config["rds_host"],
                        self.db_config["rds_port"],
                    ),
                    local_bind_address=("127.0.0.1", 0),
                )

                self.tunnel.start()

                conn_string = (
                    f"host=127.0.0.1 "
                    f"port={self.tunnel.local_bind_port} "
                    f"dbname={self.db_config['db_name']} "
                    f"user={self.db_config['db_user']} "
                    f"password={self.db_config['db_password']}"
                )

                self.connection = psycopg2.connect(conn_string, cursor_factory=DictCursor)
                st.success('データベース接続完了')

            return self.connection, self.tunnel

        except Exception as e:
            st.error(f"接続エラー: {str(e)}")
            raise
        finally:
            if self._temp_key_file and os.path.exists(self._temp_key_file.name):
                os.unlink(self._temp_key_file.name)
                self._temp_key_file = None

    def get_inspection_customers(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """リクエスト検品を申し込んだ顧客と商品数を取得（デバッグ情報付き）"""
        # 元の件数確認用クエリ
        debug_queries = {
            "total_inspections": """
            SELECT COUNT(*) as total
            FROM request_inspections
            """,
            
            "inspections_with_details": """
            SELECT COUNT(DISTINCT ri.id) as total
            FROM request_inspections ri
            JOIN request_inspection_details rid ON ri.id = rid."requestInspectionId"
            """,
            
            "inspections_with_complete_join": """
            SELECT COUNT(DISTINCT ri.id) as total
            FROM request_inspections ri
            JOIN request_inspection_details rid ON ri.id = rid."requestInspectionId"
            JOIN skus sk ON rid.sku = sk.sku
            JOIN specs s ON sk."specId" = s."specId"
            """,
            
            "inspections_with_orders": """
            SELECT COUNT(DISTINCT ri.id) as total
            FROM request_inspections ri
            JOIN orders o ON ri."orderId" = o."orderId"
            JOIN customers c ON o."customerId" = c."customerId"
            """,
            
            # 存在しないSKUを特定するクエリを追加
            "missing_skus": """
            SELECT DISTINCT 
                rid.sku,
                ri.id as request_inspection_id,
                ri."orderId",
                ri."createdAt"
            FROM request_inspection_details rid
            JOIN request_inspections ri ON rid."requestInspectionId" = ri.id
            LEFT JOIN skus sk ON rid.sku = sk.sku
            WHERE sk.sku IS NULL
            ORDER BY ri."createdAt" DESC
            """
        }
        
        debug_results = {}
        for name, query in debug_queries.items():
            df = pd.read_sql_query(query, self.connection)
            if name == "missing_skus":
                debug_results[name] = df
            else:
                debug_results[name] = df['total'].iloc[0]
        
        # メインのクエリ（変更なし）
        query = """
        WITH inspection_details AS (
            SELECT 
                rid.sku,
                ri."orderId",
                s.category,
                ri."createdAt",
                ARRAY_LENGTH(rid.items, 1) as items_count
            FROM request_inspection_details rid
            JOIN request_inspections ri ON rid."requestInspectionId" = ri.id
            JOIN skus sk ON rid.sku = sk.sku
            JOIN specs s ON sk."specId" = s."specId"
        )
        SELECT DISTINCT 
            c."customerId" as customer_id,
            c."createdAt" as customer_created_at,
            COUNT(DISTINCT ri.id) as total_requests,
            SUM(id.items_count) as total_items_requested,
            STRING_AGG(DISTINCT s.category, ', ') as categories,
            COUNT(DISTINCT s.category) as category_count
        FROM customers c
        JOIN orders o ON c."customerId" = o."customerId"
        JOIN request_inspections ri ON o."orderId" = ri."orderId"
        JOIN inspection_details id ON ri."orderId" = id."orderId"
        JOIN skus sk ON id.sku = sk.sku
        JOIN specs s ON sk."specId" = s."specId"
        WHERE 1=1
        """
        
        if start_date:
            query += f" AND ri.\"createdAt\" >= '{start_date}'"
        if end_date:
            query += f" AND ri.\"createdAt\" <= '{end_date}'"
            
        query += " GROUP BY c.\"customerId\", c.\"createdAt\""
        
        result_df = pd.read_sql_query(query, self.connection)
        
        # デバッグ情報を表示
        st.write("### デバッグ情報")
        st.write("各段階での検品リクエスト件数:")
        for stage, count in debug_results.items():
            if stage != "missing_skus":
                st.write(f"- {stage}: {count}件")
        
        # 存在しないSKUの情報を表示
        missing_skus_df = debug_results["missing_skus"]
        if not missing_skus_df.empty:
            st.write("\n### skusテーブルに存在しないSKU一覧:")
            st.dataframe(missing_skus_df)
            st.write(f"存在しないSKUの総数: {len(missing_skus_df)}件")
                
        return result_df, debug_results

    def analyze_repeat_customers(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> pd.DataFrame:
        """リピーター分析を実行（デバッグ情報付き）"""
        inspection_customers, debug_results = self.get_inspection_customers(start_date, end_date)
        
        results = []
        total_customers = len(inspection_customers)
        progress_bar = st.progress(0)
        
        # Shopify APIのデバッグ情報
        shopify_debug = {
            "api_errors": 0,
            "missing_orders": 0,
            "missing_metafields": 0,
            "successful_customers": 0
        }

        for idx, customer in inspection_customers.iterrows():
            progress_bar.progress((idx + 1) / total_customers)

            try:
                response = requests.post(
                    f"https://{self.shopify_config['shop_url']}/admin/api/2024-07/graphql.json",
                    json={
                        "query": """
                        query($customerId: ID!) {
                            customer(id: $customerId) {
                                orders(first: 100) {
                                    edges {
                                        node {
                                            id
                                            createdAt
                                            totalPrice
                                        }
                                    }
                                }
                                metafields(first: 10) {
                                    edges {
                                        node {
                                            key
                                            value
                                            namespace
                                        }
                                    }
                                }
                            }
                        }
                        """,
                        "variables": {
                            "customerId": f"gid://shopify/Customer/{customer['customer_id']}"
                        },
                    },
                    headers={
                        "X-Shopify-Access-Token": self.shopify_config["access_token"],
                        "Content-Type": "application/json",
                    },
                )

                if response.status_code != 200:
                    shopify_debug["api_errors"] += 1
                    st.warning(f"API Error for customer {customer['customer_id']}: Status code {response.status_code}")
                    continue

                response_data = response.json()
                if "data" not in response_data or not response_data["data"].get("customer"):
                    shopify_debug["api_errors"] += 1
                    st.warning(f"No customer data for ID {customer['customer_id']}")
                    continue

                customer_data = response_data["data"]["customer"]
                
                # ordersの確認
                orders = customer_data.get("orders", {}).get("edges", [])
                if not orders:
                    shopify_debug["missing_orders"] += 1
                    st.warning(f"No orders found for customer {customer['customer_id']}")
                    continue

                # メタフィールドの確認
                metafields = customer_data.get("metafields", {}).get("edges", [])
                if not metafields:
                    shopify_debug["missing_metafields"] += 1
                
                # メタフィールドからリピーター情報を取得
                is_aishipr_repeater = False
                for metafield in metafields:
                    if (metafield["node"]["namespace"] == "aishipr" and 
                        metafield["node"]["key"] in ["totalorders", "amountspent"] and 
                        metafield["node"]["value"]):
                        is_aishipr_repeater = True
                        break

                # 注文金額の計算
                order_amounts = [float(order["node"]["totalPrice"]) for order in orders]
                total_spent = sum(order_amounts)
                
                # 購入金額セグメントの判定
                spending_segment = (
                    "High" if total_spent > 30000 
                    else "Medium" if total_spent > 8000 
                    else "Low"
                )

                # 結果を追加
                results.append({
                    "customer_id": customer["customer_id"],
                    "total_requests": customer["total_requests"],
                    "total_items_requested": customer["total_items_requested"],
                    "categories": customer["categories"],
                    "category_count": customer["category_count"],
                    "total_orders": len(orders),
                    "total_spent": total_spent,
                    "average_order_value": total_spent / len(orders),
                    "is_repeater": is_aishipr_repeater,
                    "spending_segment": spending_segment
                })
                
                shopify_debug["successful_customers"] += 1

            except Exception as e:
                shopify_debug["api_errors"] += 1
                st.error(f"Error processing customer {customer['customer_id']}: {str(e)}")
                continue

        # デバッグ情報を表示
        st.write("### 処理状況サマリー")
        st.write(f"- DB上の検品リクエスト総数: {debug_results['total_inspections']}件")
        st.write(f"- 処理対象顧客数: {total_customers}人")
        st.write(f"- 正常処理完了顧客数: {shopify_debug['successful_customers']}人")
        st.write("")
        st.write("### Shopify API 処理結果")
        st.write(f"- API エラー数: {shopify_debug['api_errors']}件")
        st.write(f"- 注文データなし: {shopify_debug['missing_orders']}件")
        st.write(f"- メタフィールドなし: {shopify_debug['missing_metafields']}件")

        # 最終的な結果を返す
        return pd.DataFrame(results)

    def disconnect_db(self):
        """データベース接続を切断"""
        try:
            if self.connection:
                self.connection.close()
            if self.tunnel:
                self.tunnel.close()
            if self._temp_key_file and os.path.exists(self._temp_key_file.name):
                os.unlink(self._temp_key_file.name)
        except Exception as e:
            st.error(f"切断エラー: {str(e)}")


def create_basic_metrics(df: pd.DataFrame) -> None:
    """基本指標の表示"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "リクエスト検品申込数",
            f"{df['total_requests'].sum():,}件",
            help="期間内の全リクエスト検品申込数"
        )
    
    with col2:
        st.metric(
            "顧客数",
            f"{len(df):,}人",
            help="期間内のユニーク顧客数"
        )
    
    with col3:
        repeat_rate = (df['is_repeater'].sum() / len(df)) * 100
        st.metric(
            "リピート率",
            f"{repeat_rate:.1f}%",
            help="Shopifyメタフィールド（aishipr.totalorders/amountspent）に基づく実リピート率"
        )
    
    with col4:
        avg_order = df['average_order_value'].mean()
        st.metric(
            "平均注文単価",
            f"¥{avg_order:,.0f}",
            help="顧客あたりの平均注文金額"
        )

def create_category_analysis(df: pd.DataFrame) -> Tuple[go.Figure, pd.DataFrame]:
    """カテゴリ分析グラフとデータの作成"""
    # カテゴリーごとの集計
    categories = df['categories'].str.split(', ').explode()
    category_counts = categories.value_counts()
    
    # グラフの作成
    fig = px.bar(
        x=category_counts.index,
        y=category_counts.values,
        title="カテゴリー別申込数",
        labels={'x': 'カテゴリー', 'y': '申込数'},
    )
    
    fig.update_layout(
        title={
            'text': 'カテゴリー別申込数<br><span style="font-size: 12px;">（期間内の各カテゴリーの検品申込数を示します）</span>',
            'y':0.95,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'
        },
        showlegend=False
    )
    
    # 詳細データの作成
    category_detail = pd.DataFrame({
        'カテゴリー': category_counts.index,
        '申込数': category_counts.values,
        '割合': (category_counts.values / category_counts.sum() * 100).round(1)
    })
    category_detail['割合'] = category_detail['割合'].astype(str) + '%'
    
    return fig, category_detail

def create_spending_segment_analysis(df: pd.DataFrame) -> Tuple[go.Figure, pd.DataFrame]:
    """購入金額セグメント分析のグラフとデータを作成"""
    # セグメント別集計
    segment_counts = df['spending_segment'].value_counts()
    
    # グラフ作成
    fig = go.Figure(data=[
        go.Pie(
            labels=segment_counts.index,
            values=segment_counts.values,
            hole=.3,
            marker_colors=['#2E86C1', '#5DADE2', '#AED6F1']
        )
    ])
    
    fig.update_layout(
        title={
            'text': '購入金額セグメント分布<br><span style="font-size: 12px;">（High: 3万円超, Medium: 8千-3万円, Low: 8千円未満）</span>',
            'y':0.95,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'
        }
    )
    
    # セグメント別の詳細指標
    segment_metrics = df.groupby('spending_segment').agg({
        'total_requests': 'sum',
        'total_items_requested': 'sum',
        'average_order_value': 'mean',
        'is_repeater': 'mean',
        'customer_id': 'count'
    }).round(2)
    
    segment_metrics.columns = ['申込数', '商品数', '平均注文単価', 'リピート率', '顧客数']
    segment_metrics['リピート率'] = (segment_metrics['リピート率'] * 100).round(1).astype(str) + '%'
    segment_metrics['平均注文単価'] = segment_metrics['平均注文単価'].apply(lambda x: f"¥{x:,.0f}")
    
    return fig, segment_metrics

def run_streamlit_dashboard():
    st.set_page_config(page_title="リクエスト検品分析ダッシュボード", layout="wide")
    st.title("リクエスト検品分析ダッシュボード")

    if 'analyzer' not in st.session_state:
        st.session_state.analyzer = None
        st.session_state.df = None

    with st.sidebar:
        st.header("接続設定")
        
        # データベース接続情報
        db_config = {
            "bastion_host": st.text_input("Bastion Host", value="35.79.183.232"),
            "bastion_user": st.text_input("Bastion User", value="ec2-user"),
            "rds_host": st.text_input("RDS Host", value="ls-app.cysyyl7acyzm.ap-northeast-1.rds.amazonaws.com"),
            "rds_port": st.number_input("RDS Port", value=5432),
            "db_name": st.text_input("Database Name", value="ls_app_db_stg"),
            "db_user": st.text_input("Database User", value="postgres"),
            "db_password": st.text_input("Database Password", type="password")
        }

        # SSH Key アップロード
        ssh_key_file = st.file_uploader("SSH秘密鍵ファイル", type=['pem'])
        if ssh_key_file is not None:
            ssh_key_content = ssh_key_file.getvalue().decode()
        else:
            ssh_key_content = None

        # Shopify API設定
        shopify_config = {
            "shop_url": st.text_input("Shop URL", value="linea-storia.myshopify.com"),
            "access_token": st.text_input("Access Token", value="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", type="password")
        }

        st.header("期間設定")
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "開始日",
                datetime.now() - timedelta(days=30)
            )
        with col2:
            end_date = st.date_input(
                "終了日",
                datetime.now()
            )
        
        if st.button("分析実行"):
            if not db_config["db_password"]:
                st.error("データベースパスワードを入力してください")
                return
                
            if not ssh_key_content:
                st.error("SSH秘密鍵ファイルをアップロードしてください")
                return

            try:
                st.session_state.analyzer = ShopifyAnalyzer(db_config, shopify_config, ssh_key_content)
                st.session_state.analyzer.connect_db()

                st.session_state.df = st.session_state.analyzer.analyze_repeat_customers(
                    start_date=datetime.combine(start_date, datetime.min.time()),
                    end_date=datetime.combine(end_date, datetime.max.time())
                )

            except Exception as e:
                st.error(f"エラーが発生しました: {str(e)}")
            finally:
                if st.session_state.analyzer:
                    st.session_state.analyzer.disconnect_db()
    
    # メインコンテンツの表示
    if st.session_state.df is not None:
        # 基本指標
        st.header("基本指標")
        create_basic_metrics(st.session_state.df)
        
        # 期間の表示
        st.caption(
            f"分析期間: {start_date.strftime('%Y年%m月%d日')} から "
            f"{end_date.strftime('%Y年%m月%d日')} まで"
        )
        
        # タブ設定
        tab1, tab2, tab3 = st.tabs([
            "カテゴリー分析", "セグメント分析", "詳細データ"
        ])
        
        with tab1:
            st.subheader("カテゴリー分析")
            
            # 説明文
            st.markdown("""
            各商品カテゴリーの検品申込状況を分析します。
            - 申込数：各カテゴリーの検品申込総数
            - 割合：全申込数に対する各カテゴリーの割合
            """)
            
            # グラフとデータの表示
            cat_fig, cat_detail = create_category_analysis(st.session_state.df)
            st.plotly_chart(cat_fig, use_container_width=True)
            
            # 詳細データの表示
            st.subheader("カテゴリー別詳細")
            st.dataframe(
                cat_detail.style.set_properties(**{'text-align': 'center'})
            )
        
        with tab2:
            st.subheader("購入金額セグメント分析")
            
            # 説明文
            st.markdown("""
            購入金額によって顧客を3つのセグメントに分類し、分析します。
            - High: 30,000円超
            - Medium: 8,000円～30,000円
            - Low: 8,000円未満
            """)
            
            col1, col2 = st.columns([1, 1])
            
            with col1:
                # セグメント分布のグラフ
                seg_fig, seg_metrics = create_spending_segment_analysis(st.session_state.df)
                st.plotly_chart(seg_fig, use_container_width=True)
            
            with col2:
                # セグメント別指標
                st.subheader("セグメント別指標")
                st.dataframe(
                    seg_metrics.style.set_properties(**{'text-align': 'center'})
                )
            
            # セグメント別の特徴解説
            st.markdown("### セグメント別の特徴")
            high_segment = seg_metrics.loc['High']
            med_segment = seg_metrics.loc['Medium']
            
            st.markdown(f"""
            #### Highセグメント（30,000円超）
            - 顧客数: {high_segment['顧客数']:,}人
            - リピート率: {high_segment['リピート率']}
            - 平均注文単価: {high_segment['平均注文単価']}
            
            #### Mediumセグメント（8,000円～30,000円）
            - 顧客数: {med_segment['顧客数']:,}人
            - リピート率: {med_segment['リピート率']}
            - 平均注文単価: {med_segment['平均注文単価']}
            """)
        
        with tab3:
            st.subheader("詳細データ")
            
            # カラム選択
            display_columns = st.multiselect(
                "表示するカラムを選択",
                st.session_state.df.columns.tolist(),
                default=[
                    'customer_id',
                    'total_requests',
                    'total_items_requested',
                    'total_spent',
                    'average_order_value',
                    'spending_segment',
                    'categories',
                    'is_repeater'
                ]
            )
            
            # 列名の日本語変換マッピング
            column_names = {
                'customer_id': '顧客ID',
                'total_requests': '検品申込数',
                'total_items_requested': '商品数',
                'total_spent': '購入総額',
                'average_order_value': '平均注文単価',
                'spending_segment': '購入金額セグメント',
                'categories': 'カテゴリー',
                'is_repeater': 'リピーター'
            }
            
            if display_columns:
                # データフレームの表示用にコピーを作成
                display_df = st.session_state.df[display_columns].copy()
                
                # 列名を日本語に変換
                display_df.columns = [column_names.get(col, col) for col in display_df.columns]
                
                # データテーブル表示
                st.dataframe(
                    display_df.style.format({
                        '購入総額': '¥{:,.0f}',
                        '平均注文単価': '¥{:,.0f}',
                        'リピーター': lambda x: 'はい' if x else 'いいえ'
                    })
                )

            # CSVダウンロード
            csv = st.session_state.df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "CSVダウンロード",
                csv,
                "customer_analysis.csv",
                "text/csv",
                key='download-csv'
            )


if __name__ == "__main__":
    run_streamlit_dashboard()