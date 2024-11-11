import pandas as pd
import psycopg2
import sshtunnel
from psycopg2.extras import DictCursor
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import requests
from typing import Dict, Tuple, Any, Optional
import tempfile
import os
import stat
import streamlit as st
from streamlit_extras.metric_cards import style_metric_cards
from dotenv import load_dotenv

load_dotenv()

SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
SHOP_URL = os.getenv("SHOP_URL")
BASTION_HOST = os.getenv("BASTION_HOST")
BASTION_USER = os.getenv("BASTION_USER")
RDS_HOST = os.getenv("RDS_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")


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

            with st.spinner("データベースに接続中..."):
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

                self.connection = psycopg2.connect(
                    conn_string, cursor_factory=DictCursor
                )
                st.success("データベース接続完了")

            return self.connection, self.tunnel

        except Exception as e:
            st.error(f"接続エラー: {str(e)}")
            raise
        finally:
            if self._temp_key_file and os.path.exists(self._temp_key_file.name):
                os.unlink(self._temp_key_file.name)
                self._temp_key_file = None

    def get_inspection_customers(
        self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        debug_queries = {
            "total_inspections": """
            SELECT COUNT(*) as total
            FROM request_inspections
            """,
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
            """,
        }

        debug_results = {}
        for name, query in debug_queries.items():
            df = pd.read_sql_query(query, self.connection)
            if name == "missing_skus":
                debug_results[name] = df
            else:
                debug_results[name] = df["total"].iloc[0]

        query = """
        WITH inspection_details AS (
            SELECT 
                rid.sku,
                ri."orderId",
                s.category,
                ri."createdAt" AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Tokyo' as "createdAt",
                ARRAY_LENGTH(rid.items, 1) as items_count
            FROM request_inspection_details rid
            JOIN request_inspections ri ON rid."requestInspectionId" = ri.id
            JOIN skus sk ON rid.sku = sk.sku
            JOIN specs s ON sk."specId" = s."specId"
        )
        SELECT DISTINCT 
            c."customerId" as customer_id,
            c."createdAt" AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Tokyo' as customer_created_at,
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
            query += f" AND ri.\"createdAt\" AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Tokyo' >= '{start_date}'"
        if end_date:
            query += f" AND ri.\"createdAt\" AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Tokyo' <= '{end_date}'"

        query += ' GROUP BY c."customerId", c."createdAt"'

        result_df = pd.read_sql_query(query, self.connection)

        missing_skus_df = debug_results["missing_skus"]
        if not missing_skus_df.empty:
            st.write("\n### skusテーブルに存在しないSKU一覧:")
            st.dataframe(missing_skus_df)
            st.write(f"存在しないSKUの総数: {len(missing_skus_df)}件")

        return result_df, debug_results

    def get_order_statistics(
        self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        start_str = (
            (start_date.strftime("%Y-%m-%d") + "T00:00:00")
            if start_date
            else "2024-08-07T00:00:00"
        )
        end_str = (
            (end_date.strftime("%Y-%m-%d") + "T23:59:59")
            if end_date
            else datetime.now().strftime("%Y-%m-%d") + "T23:59:59"
        )

        query = """
        query getOrders($first: Int!, $after: String) {
            orders(
                first: $first,
                after: $after,
                query: "created_at:>='{start}' AND created_at:<='{end}'"
            ) {
                edges {
                    node {
                        id
                        name
                        createdAt
                        customer {
                            id
                            state
                        }
                        lineItems(first: 50) {
                            edges {
                                node {
                                    product {
                                        id
                                        metafield(namespace: "custom", key: "request_inspection_pattern") {
                                            value
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
        """

        orders_data = []
        has_next = True
        cursor = None

        query = query.replace("{start}", start_str).replace("{end}", end_str)

        while has_next:
            try:
                response = requests.post(
                    f"https://{self.shopify_config['shop_url']}/admin/api/2024-07/graphql.json",
                    json={"query": query, "variables": {"first": 100, "after": cursor}},
                    headers={
                        "X-Shopify-Access-Token": self.shopify_config["access_token"],
                        "Content-Type": "application/json",
                    },
                )

                if response.status_code != 200:
                    st.error(f"API Error: Status code {response.status_code}")
                    break

                data = response.json()
                if "errors" in data:
                    st.error(f"GraphQL Error: {data['errors']}")
                    break

                page_data = data["data"]["orders"]
                orders_data.extend(page_data["edges"])

                page_info = page_data["pageInfo"]
                has_next = page_info["hasNextPage"]
                cursor = page_info["endCursor"]

            except Exception as e:
                st.error(f"Error fetching orders: {str(e)}")
                break

        total_orders = len(orders_data)
        customers = set()
        inspection_eligible_orders = 0
        member_stats = {
            "ENABLED": {"orders": 0, "customers": set(), "repeat_customers": set()},
            "INVITED": {"orders": 0, "customers": set()},
            "DISABLED": {"orders": 0, "customers": set()},
            "DECLINED": {"orders": 0, "customers": set()},
            "GUEST": {"orders": 0, "customers": set()},
        }

        # 顧客ごとの注文数をカウント
        customer_order_counts = {}

        for order in orders_data:
            order_node = order["node"]
            customer = order_node.get("customer")

            # 会員判定と注文数カウント
            if customer:
                customer_id = customer["id"]
                state = customer.get("state", "GUEST")

                if state in member_stats:
                    member_stats[state]["orders"] += 1
                    member_stats[state]["customers"].add(customer_id)
                    customers.add(customer_id)

                    # 注文数をカウント
                    if state == "ENABLED":
                        customer_order_counts[customer_id] = (
                            customer_order_counts.get(customer_id, 0) + 1
                        )
                        if customer_order_counts[customer_id] > 1:
                            member_stats[state]["repeat_customers"].add(customer_id)
            else:
                member_stats["GUEST"]["orders"] += 1

            # 検品対象商品判定
            for item in order_node["lineItems"]["edges"]:
                product = item["node"]["product"]
                if not product:
                    continue

                metafield = product.get("metafield")
                if metafield and metafield.get("value") in ["patternA", "patternB"]:
                    inspection_eligible_orders += 1
                    break

        stats = {
            "total_orders": total_orders,
            "total_customers": len(customers),
            "member_stats": {
                state: {
                    "orders": data["orders"],
                    "customers": len(data["customers"]),
                    "repeat_customers": len(data.get("repeat_customers", set())),
                    "order_ratio": (data["orders"] / total_orders * 100)
                    if total_orders > 0
                    else 0,
                    "customer_ratio": (len(data["customers"]) / len(customers) * 100)
                    if customers
                    else 0,
                }
                for state, data in member_stats.items()
            },
            "inspection_eligible_orders": inspection_eligible_orders,
            "inspection_eligible_ratio": (
                inspection_eligible_orders / total_orders * 100
            )
            if total_orders > 0
            else 0,
        }

        return stats

    def analyze_repeat_customers(
        self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        inspection_customers, debug_results = self.get_inspection_customers(
            start_date, end_date
        )

        results = []
        total_customers = len(inspection_customers)
        progress_bar = st.progress(0)

        shopify_debug = {
            "api_errors": 0,
            "missing_orders": 0,
            "missing_metafields": 0,
            "successful_customers": 0,
        }

        prev_inspection_query = """
        SELECT DISTINCT c."customerId"
        FROM customers c
        JOIN orders o ON c."customerId" = o."customerId"
        JOIN request_inspections ri ON o."orderId" = ri."orderId"
        WHERE ri."createdAt" < %s
        """

        prev_inspection_customers = set()
        with self.connection.cursor() as cursor:
            cursor.execute(prev_inspection_query, (start_date,))
            prev_inspection_customers = {row[0] for row in cursor.fetchall()}

        for idx, customer in inspection_customers.iterrows():
            progress_bar.progress((idx + 1) / total_customers)

            try:
                response = requests.post(
                    f"https://{self.shopify_config['shop_url']}/admin/api/2024-07/graphql.json",
                    json={
                        "query": """
                        query($customerId: ID!) {
                            customer(id: $customerId) {
                                state  # 追加
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
                    st.warning(
                        f"API Error for customer {customer['customer_id']}: Status code {response.status_code}"
                    )
                    continue

                response_data = response.json()
                if "data" not in response_data or not response_data["data"].get(
                    "customer"
                ):
                    shopify_debug["api_errors"] += 1
                    st.warning(f"No customer data for ID {customer['customer_id']}")
                    continue

                customer_data = response_data["data"]["customer"]

                orders = customer_data.get("orders", {}).get("edges", [])
                if not orders:
                    shopify_debug["missing_orders"] += 1
                    st.warning(
                        f"No orders found for customer {customer['customer_id']}"
                    )
                    continue

                metafields = customer_data.get("metafields", {}).get("edges", [])
                if not metafields:
                    shopify_debug["missing_metafields"] += 1

                # メタフィールドからリピーター情報を取得
                is_shopify_repeater = False
                for metafield in metafields:
                    if (
                        metafield["node"]["namespace"] == "aishipr"
                        and metafield["node"]["key"] in ["totalorders", "amountspent"]
                        and metafield["node"]["value"]
                    ):
                        is_shopify_repeater = True
                        break

                # 過去の注文履歴があるかチェック（注文回数が1より大きい）
                has_previous_orders = len(orders) > 1

                # リピーター判定（どちらかの条件を満たせばリピーター）
                is_repeat_customer = is_shopify_repeater or has_previous_orders

                # 注文金額の計算
                order_amounts = [float(order["node"]["totalPrice"]) for order in orders]
                total_spent = sum(order_amounts)

                spending_segment = (
                    "High"
                    if total_spent > 30000
                    else "Medium"
                    if total_spent > 8000
                    else "Low"
                )

                customer_state = customer_data.get("state", "GUEST")

                results.append(
                    {
                        "customer_id": customer["customer_id"],
                        "total_requests": customer["total_requests"],
                        "total_items_requested": customer["total_items_requested"],
                        "categories": customer["categories"],
                        "category_count": customer["category_count"],
                        "total_orders": len(orders),
                        "total_spent": total_spent,
                        "average_order_value": total_spent / len(orders),
                        "is_shopify_repeater": is_shopify_repeater,
                        "has_previous_orders": has_previous_orders,
                        "is_repeat_customer": is_repeat_customer,
                        "spending_segment": spending_segment,
                        "customer_state": customer_state,
                        "has_previous_inspection": customer["customer_id"]
                        in prev_inspection_customers,
                    }
                )

                shopify_debug["successful_customers"] += 1

            except Exception as e:
                shopify_debug["api_errors"] += 1
                st.error(
                    f"Error processing customer {customer['customer_id']}: {str(e)}"
                )
                continue

        return pd.DataFrame(results)

    def disconnect_db(self):
        try:
            if self.connection:
                self.connection.close()
            if self.tunnel:
                self.tunnel.close()
            if self._temp_key_file and os.path.exists(self._temp_key_file.name):
                os.unlink(self._temp_key_file.name)
        except Exception as e:
            st.error(f"切断エラー: {str(e)}")


def create_basic_metrics(df: pd.DataFrame, order_stats: Dict[str, Any]) -> None:
    """基本的な注文情報の指標を表示する"""
    # タブを作成（リストとして返されるので、最初の要素を取得）
    [tab_order] = st.tabs(["注文情報"])

    with tab_order:
        # 注文情報
        row1_col1, row1_col2 = st.columns(2)
        with row1_col1:
            st.metric(
                "総注文数",
                f"{order_stats['total_orders']:,}件",
                help="分析期間内に受注した全注文の総数",
            )
        with row1_col2:
            st.metric(
                "会員顧客数",
                f"{order_stats['total_customers']:,}人",
                help="分析期間内に注文をした会員（有効、招待中、無効化された会員）のユニーク人数",
            )

        row2_col1, row2_col2 = st.columns(2)
        with row2_col1:
            st.metric(
                "ゲスト注文数",
                f"{order_stats['member_stats']['GUEST']['orders']:,}件",
                help="分析期間内の非会員（ゲスト）による注文数",
            )

        with row2_col2:
            st.metric(
                "リクエスト検品対象商品を含む注文",
                f"{order_stats['inspection_eligible_orders']:,}件 ({order_stats['inspection_eligible_ratio']:.1f}%)",
                help="リクエスト検品の対象商品を含む注文数とその割合",
            )

        # 会員状態別の内訳
        st.markdown("#### 会員状態別の内訳")
        member_stats = order_stats["member_stats"]
        member_data = {
            "区分": ["有効な会員", "招待中", "無効化された会員", "招待拒否", "ゲスト"],
            "注文数": [
                member_stats["ENABLED"]["orders"],
                member_stats["INVITED"]["orders"],
                member_stats["DISABLED"]["orders"],
                member_stats["DECLINED"]["orders"],
                member_stats["GUEST"]["orders"],
            ],
            "注文割合": [
                f"{member_stats['ENABLED']['order_ratio']:.1f}%",
                f"{member_stats['INVITED']['order_ratio']:.1f}%",
                f"{member_stats['DISABLED']['order_ratio']:.1f}%",
                f"{member_stats['DECLINED']['order_ratio']:.1f}%",
                f"{member_stats['GUEST']['order_ratio']:.1f}%",
            ],
            "顧客数": [
                member_stats["ENABLED"]["customers"],
                member_stats["INVITED"]["customers"],
                member_stats["DISABLED"]["customers"],
                member_stats["DECLINED"]["customers"],
                member_stats["GUEST"]["customers"],
            ],
            "顧客割合": [
                f"{member_stats['ENABLED']['customer_ratio']:.1f}%",
                f"{member_stats['INVITED']['customer_ratio']:.1f}%",
                f"{member_stats['DISABLED']['customer_ratio']:.1f}%",
                f"{member_stats['DECLINED']['customer_ratio']:.1f}%",
                f"{member_stats['GUEST']['customer_ratio']:.1f}%",
            ],
        }
        st.dataframe(
            pd.DataFrame(member_data).style.set_properties(**{"text-align": "center"})
        )

    style_metric_cards()


def create_inspection_analysis(df: pd.DataFrame, order_stats: Dict[str, Any]) -> None:
    member_stats = order_stats["member_stats"]

    # 会員（有効な会員）とリピーター情報
    total_enabled = member_stats["ENABLED"]["customers"]
    total_repeaters = member_stats["ENABLED"]["repeat_customers"]
    total_new = total_enabled - total_repeaters

    # 非会員（ゲスト）の総数
    total_guest = member_stats["GUEST"]["customers"]

    # 会員・非会員データの分離
    member_df = df[df["customer_state"] == "ENABLED"]
    guest_df = df[df["customer_state"] == "GUEST"]

    # 検品申込ありの新規・リピーター
    with_inspection_repeat = len(member_df[member_df["is_repeat_customer"]])
    with_inspection_new = len(member_df[~member_df["is_repeat_customer"]])

    # 会員の指標計算
    member_metrics = {
        "total_customers": total_enabled,
        "total_repeaters": total_repeaters,
        "total_new": total_new,
        "with_inspection": len(member_df),
        "without_inspection": total_enabled - len(member_df),
        "inspection_rate": (len(member_df) / total_enabled * 100)
        if total_enabled > 0
        else 0,
        # 検品申込ありの内訳
        "with_inspection_repeat": with_inspection_repeat,
        "with_inspection_new": with_inspection_new,
        # 検品申込なしの内訳
        "without_inspection_repeat": total_repeaters - with_inspection_repeat,
        "without_inspection_new": total_new - with_inspection_new,
        # 継続率関連
        "continued_inspection": len(member_df[member_df["has_previous_inspection"]]),
        "discontinued_inspection": len(
            member_df[
                ~member_df["has_previous_inspection"] & member_df["is_repeat_customer"]
            ]
        ),
    }

    # 非会員の指標計算
    guest_metrics = {
        "total_customers": total_guest,
        "with_inspection": len(guest_df),
        "without_inspection": total_guest - len(guest_df),
        "inspection_rate": (len(guest_df) / total_guest * 100)
        if total_guest > 0
        else 0,
        "repeat_customers": len(guest_df[guest_df["is_repeat_customer"]]),
        "new_customers": len(guest_df[~guest_df["is_repeat_customer"]]),
    }

    # タブの作成
    tab_summary, tab_member, tab_guest = st.tabs(
        ["総合サマリー", "会員分析", "非会員分析"]
    )

    # 総合サマリータブ
    with tab_summary:
        st.subheader("リクエスト検品申込の全体概要")

        # メトリクスの表示
        col1, col2 = st.columns(2)
        with col1:
            st.metric(
                "会員の検品申込率",
                f"{member_metrics['inspection_rate']:.1f}%",
                help="有効な会員のうち、リクエスト検品を申し込んだ割合",
            )
        with col2:
            st.metric(
                "非会員の検品申込率",
                f"{guest_metrics['inspection_rate']:.1f}%",
                help="非会員（ゲスト）購入者のうち、リクエスト検品を申し込んだ割合",
            )

        # 全体の申込状況グラフ
        inspection_data = pd.DataFrame(
            {
                "区分": ["会員", "非会員"],
                "申込あり": [
                    member_metrics["with_inspection"],
                    guest_metrics["with_inspection"],
                ],
                "申込なし": [
                    member_metrics["without_inspection"],
                    guest_metrics["without_inspection"],
                ],
            }
        )

        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                name="申込あり",
                x=inspection_data["区分"],
                y=inspection_data["申込あり"],
                marker_color="#F5B79E",
            )
        )
        fig.add_trace(
            go.Bar(
                name="申込なし",
                x=inspection_data["区分"],
                y=inspection_data["申込なし"],
                marker_color="#E0E0E0",
            )
        )

        fig.update_layout(
            title="会員・非会員別の検品申込状況",
            barmode="stack",
            yaxis_title="顧客数",
            showlegend=True,
        )

        st.plotly_chart(fig, use_container_width=True, key="summary_chart")

    # 会員分析タブ
    with tab_member:
        st.subheader("会員（有効な会員）の検品申込分析")

        # 1. 検品申込の有無の全体像
        col1, col2 = st.columns(2)

        with col1:
            st.metric(
                "検品申込率",
                f"{member_metrics['inspection_rate']:.1f}%",
                help="有効な会員全体に対する検品申込のあった会員の割合",
            )

            # 申込状況の内訳を表で表示
            inspection_overview = pd.DataFrame(
                {
                    "区分": ["申込あり", "申込なし"],
                    "顧客数": [
                        member_metrics["with_inspection"],
                        member_metrics["without_inspection"],
                    ],
                    "割合": [
                        f"{(member_metrics['with_inspection']/member_metrics['total_customers']*100):.1f}%",
                        f"{(member_metrics['without_inspection']/member_metrics['total_customers']*100):.1f}%",
                    ],
                }
            )
            st.write("##### 検品申込状況")
            st.dataframe(
                inspection_overview.style.set_properties(**{"text-align": "center"}),
                use_container_width=True,
            )

        with col2:
            # 申込有無の内訳を円グラフで表示
            fig = px.pie(
                values=[
                    member_metrics["with_inspection"],
                    member_metrics["without_inspection"],
                ],
                names=["申込あり", "申込なし"],
                title="検品申込状況の分布",
                color_discrete_sequence=["#F5B79E", "#E0E0E0"],
            )
            st.plotly_chart(
                fig, use_container_width=True, key="inspection_distribution"
            )

        # 2. 申込ありの詳細分析
        st.write("### 検品申込あり会員の分析")
        col1, col2, col3 = st.columns(3)

        with col1:
            # 新規会員の申込率
            new_rate = (
                member_metrics["with_inspection_new"]
                / member_metrics["with_inspection"]
                * 100
                if member_metrics["with_inspection"] > 0
                else 0
            )
            st.metric(
                "新規会員の割合",
                f"{new_rate:.1f}%",
                help="検品申込のある会員のうち、新規会員の割合",
            )

        with col2:
            # リピーター会員の申込率
            repeat_rate = (
                member_metrics["with_inspection_repeat"]
                / member_metrics["with_inspection"]
                * 100
                if member_metrics["with_inspection"] > 0
                else 0
            )
            st.metric(
                "リピーターの割合",
                f"{repeat_rate:.1f}%",
                help="検品申込のある会員のうち、リピーターの割合",
            )

        with col3:
            # 検品継続率
            continuation_rate = (
                member_metrics["continued_inspection"]
                / (
                    member_metrics["continued_inspection"]
                    + member_metrics["discontinued_inspection"]
                )
                * 100
                if (
                    member_metrics["continued_inspection"]
                    + member_metrics["discontinued_inspection"]
                )
                > 0
                else 0
            )
            st.metric(
                "検品継続率",
                f"{continuation_rate:.1f}%",
                help="過去に検品申込があったリピーター会員のうち、今回も申し込んだ会員の割合",
            )

        # 申込ありの内訳を表とグラフで表示
        col1, col2 = st.columns(2)

        with col1:
            with_inspection_detail = pd.DataFrame(
                {
                    "区分": ["新規", "リピーター"],
                    "顧客数": [
                        member_metrics["with_inspection_new"],
                        member_metrics["with_inspection_repeat"],
                    ],
                    "割合": [f"{new_rate:.1f}%", f"{repeat_rate:.1f}%"],
                }
            )
            st.write("##### 検品申込あり会員の内訳")
            st.dataframe(
                with_inspection_detail.style.set_properties(**{"text-align": "center"}),
                use_container_width=True,
            )

        with col2:
            fig = px.pie(
                values=[
                    member_metrics["with_inspection_new"],
                    member_metrics["with_inspection_repeat"],
                ],
                names=["新規", "リピーター"],
                title="検品申込あり会員の分布",
                color_discrete_sequence=["#F5B79E", "#F8CBBE"],
            )
            st.plotly_chart(
                fig, use_container_width=True, key="with_inspection_distribution"
            )

        # 3. 申込なしの詳細分析
        st.write("### 検品申込なし会員の分析")

        # 申込なしの内訳を表とグラフで表示
        col1, col2 = st.columns(2)

        with col1:
            no_inspection_detail = pd.DataFrame(
                {
                    "区分": ["新規", "リピーター"],
                    "顧客数": [
                        member_metrics["without_inspection_new"],
                        member_metrics["without_inspection_repeat"],
                    ],
                    "割合": [
                        f"{(member_metrics['without_inspection_new']/member_metrics['without_inspection']*100):.1f}%"
                        if member_metrics["without_inspection"] > 0
                        else "0.0%",
                        f"{(member_metrics['without_inspection_repeat']/member_metrics['without_inspection']*100):.1f}%"
                        if member_metrics["without_inspection"] > 0
                        else "0.0%",
                    ],
                }
            )
            st.write("##### 検品申込なし会員の内訳")
            st.dataframe(
                no_inspection_detail.style.set_properties(**{"text-align": "center"}),
                use_container_width=True,
            )

        with col2:
            fig = px.pie(
                values=[
                    member_metrics["without_inspection_new"],
                    member_metrics["without_inspection_repeat"],
                ],
                names=["新規", "リピーター"],
                title="検品申込なし会員の分布",
                color_discrete_sequence=["#FBE0D8", "#E0E0E0"],
            )
            st.plotly_chart(
                fig, use_container_width=True, key="without_inspection_distribution"
            )

    # 非会員分析タブ
    with tab_guest:
        st.subheader("非会員（ゲスト）の検品申込分析")

        # メトリクス表示
        col1, col2 = st.columns(2)
        with col1:
            st.metric(
                "検品申込率",
                f"{guest_metrics['inspection_rate']:.1f}%",
                help="非会員の購入者のうち、検品申込をした割合",
            )

            # 申込状況の内訳を表で表示
            guest_overview = pd.DataFrame(
                {
                    "区分": ["申込あり", "申込なし"],
                    "顧客数": [
                        guest_metrics["with_inspection"],
                        guest_metrics["without_inspection"],
                    ],
                    "割合": [
                        f"{(guest_metrics['with_inspection']/guest_metrics['total_customers']*100):.1f}%"
                        if guest_metrics["total_customers"] > 0
                        else "0.0%",
                        f"{(guest_metrics['without_inspection']/guest_metrics['total_customers']*100):.1f}%"
                        if guest_metrics["total_customers"] > 0
                        else "0.0%",
                    ],
                }
            )
            st.write("##### 検品申込状況")
            st.dataframe(
                guest_overview.style.set_properties(**{"text-align": "center"}),
                use_container_width=True,
            )

        with col2:
            # 申込有無の分布
            fig = px.pie(
                values=[
                    guest_metrics["with_inspection"],
                    guest_metrics["without_inspection"],
                ],
                names=["申込あり", "申込なし"],
                title="非会員の検品申込状況",
                color_discrete_sequence=["#F5B79E", "#E0E0E0"],
            )
            st.plotly_chart(fig, use_container_width=True, key="guest_distribution")

        # 申込ありの分析
        st.write("### 検品申込あり非会員の分析")

        col1, col2 = st.columns(2)

        with col1:
            # 新規とリピーターの割合を計算
            new_rate = (
                guest_metrics["new_customers"] / guest_metrics["with_inspection"] * 100
                if guest_metrics["with_inspection"] > 0
                else 0
            )
            repeat_rate = (
                guest_metrics["repeat_customers"]
                / guest_metrics["with_inspection"]
                * 100
                if guest_metrics["with_inspection"] > 0
                else 0
            )

            guest_detail = pd.DataFrame(
                {
                    "区分": ["新規", "リピーター"],
                    "顧客数": [
                        guest_metrics["new_customers"],
                        guest_metrics["repeat_customers"],
                    ],
                    "割合": [f"{new_rate:.1f}%", f"{repeat_rate:.1f}%"],
                }
            )
            st.write("##### 検品申込あり非会員の内訳")
            st.dataframe(
                guest_detail.style.set_properties(**{"text-align": "center"}),
                use_container_width=True,
            )

        with col2:
            # 申込ありの新規・リピーター分布
            fig = px.pie(
                values=[
                    guest_metrics["new_customers"],
                    guest_metrics["repeat_customers"],
                ],
                names=["新規", "リピーター"],
                title="検品申込あり非会員の分布",
                color_discrete_sequence=["#F5B79E", "#F8CBBE"],
            )
            st.plotly_chart(
                fig, use_container_width=True, key="guest_type_distribution"
            )

        # 新規・リピーターのメトリクス表示
        col1, col2 = st.columns(2)
        with col1:
            st.metric(
                "新規非会員の割合",
                f"{new_rate:.1f}%",
                help="検品申込のある非会員のうち、初回購入者の割合",
            )
        with col2:
            st.metric(
                "リピーター非会員の割合",
                f"{repeat_rate:.1f}%",
                help="検品申込のある非会員のうち、リピーター購入者の割合",
            )

        # データサマリー表示（オプション）
        if st.checkbox("詳細データを表示"):
            st.write("### 非会員の詳細データ")
            summary_data = {
                "指標": [
                    "総非会員数",
                    "検品申込数",
                    "検品申込率",
                    "新規顧客数",
                    "リピーター数",
                    "新規比率",
                    "リピーター比率",
                ],
                "値": [
                    f"{guest_metrics['total_customers']:,}人",
                    f"{guest_metrics['with_inspection']:,}人",
                    f"{guest_metrics['inspection_rate']:.1f}%",
                    f"{guest_metrics['new_customers']:,}人",
                    f"{guest_metrics['repeat_customers']:,}人",
                    f"{new_rate:.1f}%",
                    f"{repeat_rate:.1f}%",
                ],
            }
            st.dataframe(
                pd.DataFrame(summary_data).style.set_properties(
                    **{"text-align": "center"}
                ),
                use_container_width=True,
            )


def create_category_analysis(df: pd.DataFrame) -> Tuple[go.Figure, pd.DataFrame]:
    categories = df["categories"].str.split(", ").explode()
    category_counts = categories.value_counts()

    fig = px.bar(
        x=category_counts.index,
        y=category_counts.values,
        title="カテゴリー別申込数",
        labels={"x": "カテゴリー", "y": "申込数"},
    )

    fig.update_layout(
        title={
            "text": 'カテゴリー別申込数<br><span style="font-size: 12px;">（期間内の各カテゴリーのリクエスト検品申込数を示します）</span>',
            "y": 0.95,
            "x": 0.5,
            "xanchor": "center",
            "yanchor": "top",
        },
        showlegend=False,
    )

    fig.update_traces(
        marker_color="#F5B79E",
        hovertemplate="カテゴリー: %{x}<br>申込数: %{y}<extra></extra>",
    )

    category_detail = pd.DataFrame(
        {
            "カテゴリー": category_counts.index,
            "申込数": category_counts.values,
            "割合": (category_counts.values / category_counts.sum() * 100).round(1),
        }
    )
    category_detail["割合"] = category_detail["割合"].astype(str) + "%"

    return fig, category_detail


def create_spending_segment_analysis(
    df: pd.DataFrame,
) -> Tuple[go.Figure, pd.DataFrame]:
    segment_counts = df["spending_segment"].value_counts()

    # セグメントの順序を指定（High → Medium → Low）
    segments = ["High", "Medium", "Low"]
    ordered_counts = pd.Series(
        [segment_counts.get(segment, 0) for segment in segments], index=segments
    )

    colors = ["#F5B79E", "#F8CBBE", "#FBE0D8"]

    # グラフ作成
    fig = go.Figure(
        data=[
            go.Pie(
                labels=ordered_counts.index,
                values=ordered_counts.values,
                hole=0.3,
                marker_colors=colors,
                sort=False,
            )
        ]
    )

    fig.update_layout(
        title={
            "text": '購入金額セグメント分布<br><span style="font-size: 12px;">（High: 3万円超, Medium: 8千-3万円, Low: 8千円未満）</span>',
            "y": 0.95,
            "x": 0.5,
            "xanchor": "center",
            "yanchor": "top",
        }
    )

    segment_metrics = (
        df.groupby("spending_segment")
        .agg(
            {
                "total_requests": "sum",
                "total_items_requested": "sum",
                "average_order_value": "mean",
                "has_previous_orders": "mean",
                "customer_id": "count",
            }
        )
        .reindex(segments)
        .round(2)
    )

    segment_metrics.columns = [
        "申込数",
        "商品数",
        "平均注文単価",
        "リピート率",
        "顧客数",
    ]
    segment_metrics["リピート率"] = (segment_metrics["リピート率"] * 100).round(
        1
    ).astype(str) + "%"
    segment_metrics["平均注文単価"] = segment_metrics["平均注文単価"].apply(
        lambda x: f"¥{x:,.0f}"
    )

    return fig, segment_metrics


def run_streamlit_dashboard():
    st.set_page_config(page_title="リクエスト検品分析ダッシュボード", layout="wide")

    # 初期化
    if "analyzer" not in st.session_state:
        st.session_state.analyzer = None
    if "df" not in st.session_state:
        st.session_state.df = None
    if "order_stats" not in st.session_state:
        st.session_state.order_stats = None
    if "analysis_date" not in st.session_state:
        st.session_state.analysis_date = None

    with st.sidebar:
        st.title("リクエスト検品分析")

        st.write("データベースに接続するためにSSH keyをアップロードしてください")
        ssh_key_file = st.file_uploader("SSH秘密鍵ファイル", type=["pem"])
        if ssh_key_file is not None:
            ssh_key_content = ssh_key_file.getvalue().decode()
        else:
            ssh_key_content = None

        st.header("期間設定")
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("開始日", datetime.now() - timedelta(days=30))
        with col2:
            end_date = st.date_input("終了日", datetime.now())

        if st.button("分析開始"):
            if not ssh_key_content:
                st.error("SSH秘密鍵ファイルをアップロードしてください")
                return

            try:
                # 既存の接続があれば切断
                if st.session_state.analyzer:
                    st.session_state.analyzer.disconnect_db()
                    st.session_state.analyzer = None

                # 新しい接続
                st.session_state.analyzer = ShopifyAnalyzer(
                    db_config={
                        "bastion_host": BASTION_HOST,
                        "bastion_user": BASTION_USER,
                        "rds_host": RDS_HOST,
                        "rds_port": 5432,
                        "db_name": DB_NAME,
                        "db_user": "postgres",
                        "db_password": DB_PASSWORD,
                    },
                    shopify_config={
                        "shop_url": SHOP_URL,
                        "access_token": SHOPIFY_ACCESS_TOKEN,
                    },
                    ssh_key_content=ssh_key_content,
                )

                # データベース接続
                st.session_state.analyzer.connect_db()

                # データ取得
                start_datetime = datetime.combine(start_date, datetime.min.time())
                end_datetime = datetime.combine(end_date, datetime.max.time())

                st.session_state.df = (
                    st.session_state.analyzer.analyze_repeat_customers(
                        start_date=start_datetime, end_date=end_datetime
                    )
                )

                st.session_state.order_stats = (
                    st.session_state.analyzer.get_order_statistics(
                        start_date=start_datetime, end_date=end_datetime
                    )
                )

                # 分析日時を保存
                st.session_state.analysis_date = {"start": start_date, "end": end_date}

            except Exception as e:
                st.error(f"エラーが発生しました: {str(e)}")
            finally:
                if st.session_state.analyzer:
                    st.session_state.analyzer.disconnect_db()

    # メインコンテンツの表示
    if (
        st.session_state.df is not None
        and st.session_state.order_stats is not None
        and st.session_state.analysis_date is not None
    ):
        st.caption(
            f"分析期間: {st.session_state.analysis_date['start'].strftime('%Y年%m月%d日')} から "
            f"{st.session_state.analysis_date['end'].strftime('%Y年%m月%d日')} まで"
        )

        tab_basic, tab_inspection, tab_category, tab_segment, tab_detail = st.tabs(
            ["基本情報", "検品分析", "カテゴリー分析", "セグメント分析", "詳細データ"]
        )

        with tab_basic:
            create_basic_metrics(st.session_state.df, st.session_state.order_stats)

        with tab_inspection:
            create_inspection_analysis(
                st.session_state.df, st.session_state.order_stats
            )

        with tab_category:
            st.subheader("カテゴリー分析")
            st.markdown("""
            各商品カテゴリーの検品申込状況を分析します。
            - 申込数：各カテゴリーの検品申込総数
            - 割合：全申込数に対する各カテゴリーの割合
            """)
            cat_fig, cat_detail = create_category_analysis(st.session_state.df)
            st.plotly_chart(cat_fig, use_container_width=True)
            st.subheader("カテゴリー別詳細")
            st.dataframe(cat_detail.style.set_properties(**{"text-align": "center"}))

        with tab_segment:
            st.subheader("購入金額セグメント分析")
            st.markdown("""
            購入金額によって顧客を3つのセグメントに分類し、分析します。
            - High: 30,000円超
            - Medium: 8,000円～30,000円
            - Low: 8,000円未満
            """)
            col1, col2 = st.columns([1, 1])
            with col1:
                seg_fig, seg_metrics = create_spending_segment_analysis(
                    st.session_state.df
                )
                st.plotly_chart(seg_fig, use_container_width=True)
            with col2:
                st.subheader("セグメント別指標")
                st.dataframe(
                    seg_metrics.style.set_properties(**{"text-align": "center"})
                )

        with tab_detail:
            st.subheader("詳細データ")

            display_columns = st.multiselect(
                "表示するカラムを選択",
                st.session_state.df.columns.tolist(),
                default=[
                    "customer_id",
                    "customer_state",
                    "total_requests",
                    "total_items_requested",
                    "total_spent",
                    "average_order_value",
                    "spending_segment",
                    "categories",
                    "has_previous_orders",
                ],
            )

            state_mapping = {
                "ENABLED": "有効な会員",
                "INVITED": "招待中",
                "DISABLED": "無効化された会員",
                "DECLINED": "招待拒否",
                "GUEST": "ゲスト",
            }

            column_names = {
                "customer_id": "顧客ID",
                "total_requests": "リクエスト検品申込数",
                "total_items_requested": "商品数",
                "total_spent": "購入総額",
                "average_order_value": "平均注文単価",
                "spending_segment": "購入金額セグメント",
                "categories": "カテゴリー",
                "has_previous_orders": "リピート購入",
                "is_shopify_repeater": "メタフィールドのリピート情報",
                "customer_state": "会員状態",
            }

            if display_columns:
                display_df = st.session_state.df[display_columns].copy()

                # 列名を日本語に変換
                display_df.columns = [
                    column_names.get(col, col) for col in display_df.columns
                ]

                st.dataframe(
                    display_df.style.format(
                        {
                            "購入総額": "¥{:,.0f}",
                            "平均注文単価": "¥{:,.0f}",
                            "リピート購入": lambda x: "はい" if x else "いいえ",
                            "メタフィールドのリピート情報": lambda x: "はい"
                            if x
                            else "いいえ",
                            "会員状態": lambda x: state_mapping.get(x, x),
                        }
                    ),
                    use_container_width=True,
                )


if __name__ == "__main__":
    run_streamlit_dashboard()
