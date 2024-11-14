import json
import gc
import pandas as pd
import psycopg2
import sshtunnel
from psycopg2.extras import DictCursor
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import requests
from typing import Dict, Tuple, Any, Optional, List
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

    def _execute_graphql_query(
        self, query: str, variables: Dict = None
    ) -> requests.Response:
        """
        Shopify Admin APIにGraphQLクエリを実行する

        Args:
            query: GraphQLクエリ文字列
            variables: クエリ変数（オプション）

        Returns:
            requests.Response: APIレスポンス
        """
        headers = {
            "X-Shopify-Access-Token": self.shopify_config["access_token"],
            "Content-Type": "application/json",
        }

        data = {"query": query, "variables": variables or {}}

        response = requests.post(
            f"https://{self.shopify_config['shop_url']}/admin/api/2024-01/graphql.json",
            json=data,
            headers=headers,
        )

        if response.status_code != 200:
            raise Exception(
                f"Shopify API error: {response.status_code} - {response.text}"
            )

        return response

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

    def get_inspection_continuity(
        self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        個人ベースの継続率分析を行う
        """
        query = """
        WITH customer_orders AS (
            SELECT 
                c."customerId",
                o."orderId",
                o."createdAt" AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Tokyo' as order_date,
                CASE WHEN ri.id IS NOT NULL THEN true ELSE false END as has_inspection,
                CASE WHEN EXISTS (
                    SELECT 1 FROM request_inspection_details rid
                    JOIN skus sk ON rid.sku = sk.sku
                    JOIN specs s ON sk."specId" = s."specId"
                    WHERE rid."requestInspectionId" = ri.id
                ) THEN true ELSE false END as is_eligible_order
            FROM customers c
            JOIN orders o ON c."customerId" = o."customerId"
            LEFT JOIN request_inspections ri ON o."orderId" = ri."orderId"
            WHERE o."createdAt" AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Tokyo' BETWEEN %s AND %s
            ORDER BY c."customerId", o."createdAt"
        )
        SELECT 
            "customerId",
            ARRAY_AGG(order_date ORDER BY order_date) as order_dates,
            ARRAY_AGG(has_inspection ORDER BY order_date) as inspection_flags,
            ARRAY_AGG(is_eligible_order ORDER BY order_date) as eligible_flags
        FROM customer_orders
        GROUP BY "customerId"
        """

        order_based_data = []  # 注文ベースの継続率
        post_first_data = []  # 開始後の継続率
        sequential_data = []  # 直前比較の継続率

        with self.connection.cursor() as cursor:
            cursor.execute(query, (start_date, end_date))
            results = cursor.fetchall()

            for customer_id, order_dates, inspection_flags, eligible_flags in results:
                # 対象商品の購入履歴がある顧客のみ分析
                if not any(eligible_flags):
                    continue

                # 1. 注文ベースの継続率
                eligible_orders = sum(eligible_flags)
                inspection_orders = sum(inspection_flags)
                if eligible_orders > 0:
                    order_based_data.append(
                        {
                            "customer_id": customer_id,
                            "eligible_orders": eligible_orders,
                            "inspection_orders": inspection_orders,
                            "rate": inspection_orders / eligible_orders,
                        }
                    )

                # 2. 開始後の継続率
                try:
                    first_inspection_idx = inspection_flags.index(True)
                    post_first_eligible = sum(
                        eligible_flags[first_inspection_idx + 1 :]
                    )
                    post_first_inspections = sum(
                        inspection_flags[first_inspection_idx + 1 :]
                    )
                    if post_first_eligible > 0:
                        post_first_data.append(
                            {
                                "customer_id": customer_id,
                                "eligible_orders": post_first_eligible,
                                "inspection_orders": post_first_inspections,
                                "rate": post_first_inspections / post_first_eligible,
                            }
                        )
                except ValueError:
                    pass  # 検品申込がない場合はスキップ

                # 3. 直前比較の継続率
                prev_inspection_idx = None
                sequential_stats = []

                for i, (is_eligible, has_inspection) in enumerate(
                    zip(eligible_flags, inspection_flags)
                ):
                    if has_inspection:
                        prev_inspection_idx = i
                    elif is_eligible and prev_inspection_idx is not None:
                        sequential_stats.append(has_inspection)

                if sequential_stats:
                    sequential_data.append(
                        {
                            "customer_id": customer_id,
                            "subsequent_orders": len(sequential_stats),
                            "continued_inspections": sum(sequential_stats),
                            "rate": sum(sequential_stats) / len(sequential_stats),
                        }
                    )

        # DataFrameに変換
        df_order_based = pd.DataFrame(order_based_data)
        df_post_first = pd.DataFrame(post_first_data)
        df_sequential = pd.DataFrame(sequential_data)

        # 3種類の継続率を計算
        order_based_stats = {
            "total_eligible": len(df_order_based),
            "total_inspections": df_order_based["inspection_orders"].sum()
            if not df_order_based.empty
            else 0,
            "continuation_rate": (
                df_order_based["inspection_orders"].sum()
                / df_order_based["eligible_orders"].sum()
                * 100
            )
            if not df_order_based.empty and df_order_based["eligible_orders"].sum() > 0
            else 0,
            "purchased_in_period": len(df_order_based),
            "continued_in_period": len(
                df_order_based[df_order_based["inspection_orders"] > 0]
            )
            if not df_order_based.empty
            else 0,
            "data": df_order_based,
        }

        post_first_stats = {
            "total_eligible": len(df_post_first),
            "total_inspections": df_post_first["inspection_orders"].sum()
            if not df_post_first.empty
            else 0,
            "continuation_rate": (
                df_post_first["inspection_orders"].sum()
                / df_post_first["eligible_orders"].sum()
                * 100
            )
            if not df_post_first.empty and df_post_first["eligible_orders"].sum() > 0
            else 0,
            "data": df_post_first,
        }

        sequential_stats = {
            "total_eligible": len(df_sequential),
            "total_inspections": df_sequential["continued_inspections"].sum()
            if not df_sequential.empty
            else 0,
            "continuation_rate": (
                df_sequential["continued_inspections"].sum()
                / df_sequential["subsequent_orders"].sum()
                * 100
            )
            if not df_sequential.empty and df_sequential["subsequent_orders"].sum() > 0
            else 0,
            "data": df_sequential,
        }

        return {
            "order_based": order_based_stats,
            "post_first": post_first_stats,
            "sequential": sequential_stats,
            # 総合的な継続率（注文ベースの継続率を使用）
            "continuation_rate": order_based_stats["continuation_rate"],
            "purchased_in_period": order_based_stats["purchased_in_period"],
            "continued_in_period": order_based_stats["continued_in_period"],
        }

    def get_customer_inspection_stats(self) -> pd.DataFrame:
        @st.cache_data(ttl=3600)
        def fetch_shopify_orders_batch(
            customer_ids: List[str], batch_start: int, batch_size: int
        ) -> pd.DataFrame:
            batch_end = min(batch_start + batch_size, len(customer_ids))
            batch = customer_ids[batch_start:batch_end]

            shopify_query = """
            query($customerIds: [ID!]!) {
                nodes(ids: $customerIds) {
                    ... on Customer {
                        id
                        orders(first: 100, query: "created_at:>=2024-08-07", sortKey: CREATED_AT, reverse: true) {
                            edges {
                                node {
                                    id
                                    createdAt
                                    lineItems(first: 1) {
                                        edges {
                                            node {
                                                product {
                                                    metafield(namespace: "custom", key: "request_inspection_pattern") {
                                                        value
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            """

            try:
                shopify_customer_ids = [
                    f"gid://shopify/Customer/{cid}" for cid in batch
                ]

                response = self._execute_graphql_query(
                    shopify_query, variables={"customerIds": shopify_customer_ids}
                )

                data = response.json()
                customer_orders = []

                if "data" in data and "nodes" in data["data"]:
                    for customer_node in data["data"]["nodes"]:
                        if not customer_node:
                            continue

                        customer_id = customer_node["id"].split("/")[-1]
                        orders = customer_node["orders"]["edges"]

                        if not orders:
                            continue

                        total_orders = len(orders)
                        eligible_orders = 0

                        # 最新の注文の情報を取得
                        latest_order = orders[0]["node"]  # orders は既にソート済み
                        latest_order_date = latest_order["createdAt"]
                        latest_order_eligible = False

                        for order in orders:
                            has_eligible_items = False
                            for item in order["node"]["lineItems"]["edges"]:
                                product = item["node"].get("product")
                                if product and product.get("metafield"):
                                    metafield = product["metafield"]
                                    if metafield and metafield.get("value"):
                                        try:
                                            pattern_data = json.loads(
                                                metafield["value"]
                                            )
                                            if pattern_data.get("enabled", False):
                                                has_eligible_items = True
                                                if (
                                                    order["node"]["id"]
                                                    == latest_order["id"]
                                                ):
                                                    latest_order_eligible = True
                                                break
                                        except json.JSONDecodeError:
                                            has_eligible_items = True
                                            if (
                                                order["node"]["id"]
                                                == latest_order["id"]
                                            ):
                                                latest_order_eligible = True
                                            break

                            if has_eligible_items:
                                eligible_orders += 1

                        customer_orders.append(
                            {
                                "customerId": customer_id,
                                "eligible_orders": eligible_orders,
                                "total_orders": total_orders,
                                "latest_order_date": latest_order_date,
                                "latest_order_eligible": latest_order_eligible,
                            }
                        )

                return pd.DataFrame(customer_orders)

            except Exception as e:
                st.error(f"Error processing batch: {str(e)}")
                return pd.DataFrame()

        # データベースから検品申込情報を取得
        query = """
        WITH eligible_orders AS (
            -- 検品対象商品が含まれる注文を時系列で取得
            SELECT 
                c."customerId",
                o."orderId",
                o."createdAt",
                CASE WHEN ri.id IS NOT NULL THEN true ELSE false END as has_inspection,
                ROW_NUMBER() OVER (PARTITION BY c."customerId" ORDER BY o."createdAt" DESC) as order_sequence
            FROM customers c
            JOIN orders o ON c."customerId" = o."customerId"
            JOIN request_inspections ri ON o."orderId" = ri."orderId"
            WHERE o."createdAt" >= '2024-08-07'
        ),
        inspection_stats AS (
            SELECT 
                c."customerId",
                COUNT(DISTINCT ri.id) as inspection_orders,
                c."createdAt" AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Tokyo' as customer_created_at,
                -- 検品対象商品が含まれる注文の履歴を取得
                ARRAY_AGG(eo."orderId" ORDER BY eo."createdAt" DESC) FILTER (WHERE eo."orderId" IS NOT NULL) as eligible_order_ids,
                ARRAY_AGG(eo.has_inspection ORDER BY eo."createdAt" DESC) FILTER (WHERE eo."orderId" IS NOT NULL) as inspection_flags
            FROM customers c
            JOIN orders o ON c."customerId" = o."customerId"
            JOIN request_inspections ri ON o."orderId" = ri."orderId"
            LEFT JOIN eligible_orders eo ON c."customerId" = eo."customerId"
            WHERE o."createdAt" >= '2024-08-07'
            GROUP BY c."customerId", c."createdAt"
            HAVING COUNT(DISTINCT ri.id) > 0
        )
        SELECT * FROM inspection_stats
        """

        with st.spinner("データベースから検品情報を取得中..."):
            df = pd.read_sql_query(query, self.connection)

        # Shopify APIからバッチ処理で注文情報を取得
        customer_ids = df["customerId"].unique().tolist()
        batch_size = 15  # バッチサイズを小さく調整
        total_batches = (len(customer_ids) + batch_size - 1) // batch_size

        orders_data = []
        progress_bar = st.progress(0, text="Shopifyから注文データを取得中...")

        for batch_idx in range(total_batches):
            batch_start = batch_idx * batch_size
            progress = (batch_idx + 1) / total_batches
            progress_bar.progress(
                progress,
                text=f"Shopifyから注文データを取得中... ({batch_idx + 1}/{total_batches})",
            )

            batch_df = fetch_shopify_orders_batch(customer_ids, batch_start, batch_size)
            if not batch_df.empty:
                orders_data.append(batch_df)

            gc.collect()

        progress_bar.empty()

        if orders_data:
            orders_df = pd.concat(orders_data, ignore_index=True)
            df = df.merge(orders_df, on="customerId", how="left")

            # データの整合性チェック
            df["eligible_orders"] = df.apply(
                lambda row: max(row["eligible_orders"], row["inspection_orders"]),
                axis=1,
            )

            # 最新注文の検品申込状況を確認
            latest_inspection_query = """
            WITH latest_orders AS (
                SELECT 
                    c."customerId",
                    o."orderId",
                    o."createdAt",
                    ROW_NUMBER() OVER (PARTITION BY c."customerId" ORDER BY o."createdAt" DESC) as rn
                FROM customers c
                JOIN orders o ON c."customerId" = o."customerId"
                WHERE o."createdAt" >= '2024-08-07'
            )
            SELECT 
                lo."customerId",
                CASE WHEN ri.id IS NOT NULL THEN true ELSE false END as has_latest_inspection
            FROM latest_orders lo
            LEFT JOIN request_inspections ri ON lo."orderId" = ri."orderId"
            WHERE lo.rn = 1
            """

            with self.connection.cursor() as cursor:
                cursor.execute(latest_inspection_query)
                latest_inspection_data = {row[0]: row[1] for row in cursor.fetchall()}

            # 最新注文の検品状況を追加（customerId を使用）
            df["latest_order_has_inspection"] = df["customerId"].map(
                latest_inspection_data
            )
            df["latest_order_eligible"] = df["latest_order_eligible"].fillna(False)

            # 離脱フラグを追加
            df["has_churned"] = (
                df["latest_order_eligible"] & ~df["latest_order_has_inspection"]
            )

            # 検品率の計算
            df["inspection_rate"] = df.apply(
                lambda row: round(
                    (row["inspection_orders"] / row["eligible_orders"] * 100), 1
                )
                if row["eligible_orders"] > 0
                else 0,
                axis=1,
            )

            # 日付のフォーマット
            df["latest_order_date"] = pd.to_datetime(
                df["latest_order_date"]
            ).dt.strftime("%Y-%m-%d")

            # 検品対象商品が含まれる注文の履歴から、最後の注文とその1つ前の注文の情報を取得
            def get_inspection_history(row):
                if len(row["eligible_order_ids"]) >= 2:
                    return {
                        "latest_eligible_has_inspection": row["inspection_flags"][0],
                        "previous_eligible_has_inspection": row["inspection_flags"][1],
                    }
                return {
                    "latest_eligible_has_inspection": row["inspection_flags"][0]
                    if len(row["inspection_flags"]) > 0
                    else False,
                    "previous_eligible_has_inspection": False,
                }

            # 履歴情報を追加
            history_info = df.apply(get_inspection_history, axis=1)
            df["latest_eligible_has_inspection"] = history_info.apply(
                lambda x: x["latest_eligible_has_inspection"]
            )
            df["previous_eligible_has_inspection"] = history_info.apply(
                lambda x: x["previous_eligible_has_inspection"]
            )

            # カラムの順序を設定
            df = df[
                [
                    "customerId",
                    "inspection_orders",
                    "total_orders",
                    "eligible_orders",
                    "inspection_rate",
                    "latest_order_date",
                    "latest_order_eligible",
                    "latest_order_has_inspection",
                    "latest_eligible_has_inspection",
                    "previous_eligible_has_inspection",
                    "has_churned",
                ]
            ].rename(columns={"customerId": "customer_id"})

        return df

    def _get_customer_eligible_orders(self, customer_id: str) -> int:
        """
        指定した顧客の検品対象商品購入数をShopify APIから取得
        """
        query = """
        query getCustomerOrders($customerId: ID!, $first: Int!) {
            customer(id: $customerId) {
                orders(first: $first) {
                    edges {
                        node {
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
                }
            }
        }
        """

        variables = {
            "customerId": customer_id,
            "first": 250,  # 取得する注文数の上限
        }

        response = self._execute_graphql_query(query, variables)
        data = response.json()

        eligible_count = 0
        if "data" in data and "customer" in data["data"] and data["data"]["customer"]:
            orders = data["data"]["customer"]["orders"]["edges"]
            for order in orders:
                has_eligible_items = False
                for item in order["node"]["lineItems"]["edges"]:
                    product = item["node"].get("product")
                    if product:
                        metafield = product.get("metafield")
                        if metafield and metafield.get("value"):
                            try:
                                pattern_data = json.loads(metafield["value"])
                                if pattern_data.get("enabled", False):
                                    has_eligible_items = True
                                    break
                            except json.JSONDecodeError:
                                has_eligible_items = True
                                break
                if has_eligible_items:
                    eligible_count += 1

        return eligible_count

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
                                        quantity
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

        # 集計処理
        total_orders = len(orders_data)
        customers = set()
        inspection_customers = set()
        eligible_customers = set()  # 検品対象商品購入顧客

        member_stats = {
            "GUEST": {"orders": 0, "customers": set(), "eligible_customers": set()},
            "MEMBER": {"orders": 0, "customers": set(), "eligible_customers": set()},
        }

        for order in orders_data:
            order_node = order["node"]
            customer = order_node["customer"]

            if not customer:
                continue

            customer_id = customer["id"]
            customers.add(customer_id)

            # 会員状態の判定
            customer_type = "GUEST" if customer["state"] == "GUEST" else "MEMBER"
            member_stats[customer_type]["orders"] += 1
            member_stats[customer_type]["customers"].add(customer_id)

            # 検品対象商品の有無をチェック
            has_eligible_items = False
            for item in order_node["lineItems"]["edges"]:
                item_node = item["node"]
                product = item_node.get("product")
                if product:
                    metafield = product.get("metafield")
                    if metafield and metafield.get("value"):
                        pattern = metafield["value"]
                        try:
                            pattern_data = json.loads(pattern)
                            if pattern_data.get("enabled", False):
                                has_eligible_items = True
                                break
                        except json.JSONDecodeError:
                            # メタフィールドの値がJSONでない場合は単純に存在確認
                            has_eligible_items = True
                            break

            if has_eligible_items:
                eligible_customers.add(customer_id)
                member_stats[customer_type]["eligible_customers"].add(customer_id)

        # 検品申込顧客の取得（既存のデータベースクエリを使用）
        inspection_query = """
        SELECT DISTINCT c."customerId"
        FROM customers c
        JOIN orders o ON c."customerId" = o."customerId"
        JOIN request_inspections ri ON o."orderId" = ri."orderId"
        WHERE o."createdAt" BETWEEN %s AND %s
        """

        inspection_customers = set()
        member_inspection_stats = {"MEMBER": set(), "GUEST": set()}

        with self.connection.cursor() as cursor:
            cursor.execute(inspection_query, (start_date, end_date))
            for row in cursor.fetchall():
                customer_id = row[0]
                inspection_customers.add(customer_id)
                # 会員状態はShopifyのデータから判定
                customer_type = (
                    "GUEST"
                    if customer_id in member_stats["GUEST"]["customers"]
                    else "MEMBER"
                )
                member_inspection_stats[customer_type].add(customer_id)

        # 0除算を防ぐための安全なデータ作成
        member_inspection_customers = len(member_inspection_stats["MEMBER"])
        guest_inspection_customers = len(member_inspection_stats["GUEST"])

        member_eligible = max(1, len(member_stats["MEMBER"]["eligible_customers"]))
        guest_eligible = max(1, len(member_stats["GUEST"]["eligible_customers"]))
        total_eligible = max(1, len(eligible_customers))

        return {
            "total_orders": total_orders,
            "total_customers": len(customers),
            "eligible_customers": len(eligible_customers),
            "inspection_customers": len(inspection_customers),
            "inspection_rate": (len(inspection_customers) / total_eligible * 100),
            "member_metrics": {
                "total_customers": len(member_stats["MEMBER"]["customers"]),
                "eligible_customers": member_eligible,
                "inspection_customers": member_inspection_customers,
            },
            "guest_metrics": {
                "total_customers": len(member_stats["GUEST"]["customers"]),
                "eligible_customers": guest_eligible,
                "inspection_customers": guest_inspection_customers,
            },
        }

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

                # リピーター判定（どちらかの条件を満たせている）
                is_repeat_customer = is_shopify_repeater or has_previous_orders

                # 注文金額の計算
                order_amounts = [float(order["node"]["totalPrice"]) for order in orders]
                total_spent = sum(order_amounts)

                customer_state = "MEMBER" if customer_data.get("state") else "GUEST"

                if customer_state != "GUEST":
                    customer_state = "MEMBER"

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


def create_customer_inspection_stats(df: pd.DataFrame) -> None:
    """
    顧客別の検品統計を表示する
    """
    st.subheader("顧客別検品統計")

    # 基本統計
    total_customers = len(df)  # 全検品申込顧客数
    repeat_customers = df[df["inspection_orders"] >= 2]  # 2回以上検品申込がある顧客
    repeat_customer_count = len(repeat_customers)  # リピーター数

    total_inspections = df["inspection_orders"].sum()
    total_eligible = df["eligible_orders"].sum()

    # 平均検品率を計算
    avg_inspection_rate = (
        round((total_inspections / total_eligible * 100), 1)
        if total_eligible > 0
        else 0
    )

    # リピート離脱率の計算
    # 検品対象商品が含まれる最後の注文とその1回前の注文を比較
    latest_eligible_repeaters = len(
        repeat_customers[
            (
                repeat_customers["previous_eligible_has_inspection"]
            )  # 1回前の注文で検品申込あり
            & (
                repeat_customers["latest_eligible_has_inspection"].notna()
            )  # 検品対象商品が含まれる最後の注文が存在
        ]
    )

    churned_repeaters = len(
        repeat_customers[
            (
                repeat_customers["previous_eligible_has_inspection"]
            )  # 1回前の注文で検品申込あり
            & (
                repeat_customers["latest_eligible_has_inspection"].notna()
            )  # 検品対象商品が含まれる最後の注文が存在
            & (
                ~repeat_customers["latest_eligible_has_inspection"]
            )  # 最後の注文で検品申込なし
        ]
    )

    repeat_churn_rate = (
        round((churned_repeaters / latest_eligible_repeaters * 100), 1)
        if latest_eligible_repeaters > 0
        else 0
    )

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            "検品申込顧客数",
            f"{total_customers:,}人",
            f"うちリピーター: {repeat_customer_count:,}人",
            help="2024/8/7以降に検品を申し込んだことのある顧客の総数。リピーターは2回以上申込のある顧客",
        )

    with col2:
        st.metric(
            "総検品申込回数",
            f"{total_inspections:,}回",
            help="2024/8/7以降の全顧客の検品申込の合計回数",
        )

    with col3:
        st.metric(
            "検品対象商品購入総数",
            f"{total_eligible:,}回",
            help="2024/8/7以降の検品対象商品が含まれる注文の総数",
        )

    with col4:
        st.metric(
            "平均検品率",
            f"{avg_inspection_rate}%",
            help="全体の検品率（総検品申込回数/検品対象商品購入総数）",
        )

    with col5:
        st.metric(
            "リピート離脱率",
            f"{repeat_churn_rate}%",
            f"対象リピーター: {latest_eligible_repeaters:,}人",
            help="検品対象商品が含まれる最後の注文の1回前で検品申込があったが、最後の注文では申し込まなかった顧客の割合",
        )

    # 顧客データの表示
    st.markdown("#### 顧客別詳細データ")

    display_df = df.copy()
    display_df.columns = [
        "顧客ID",
        "検品申込回数",
        "総注文回数",
        "検品対象商品購入回数",
        "検品率(%)",
        "最終注文日",
        "最終注文に検品対象商品あり",
        "最終注文で検品申込あり",
        "最新の検品対象商品注文で検品申込あり",
        "前回の検品対象商品注文で検品申込あり",
        "検品離脱",
    ]

    st.dataframe(
        display_df.style.set_properties(**{"text-align": "center"}),
        use_container_width=True,
    )

    # カラムの説明を折りたたみセクションとして表示
    with st.expander("カラムの説明"):
        st.markdown("""
        - **顧客ID**: Shopifyの顧客ID
        - **検品申込回数**: 期間内の検品申込の総回数
        - **総注文回数**: 期間内の注文総数
        - **検品対象商品購入回数**: 期間内の検品対象商品を含む注文数
        - **検品率(%)**: 検品申込回数 ÷ 検品対象商品購入回数 × 100
        - **最終注文日**: 最後に注文した日付
        - **最終注文に検品対象商品あり**: 最後の注文に検品対象商品が含まれているか
        - **最終注文で検品申込あり**: 最後の注文で検品申込をしたか
        - **最新の検品対象商品注文で検品申込あり**: 検品対象商品が含まれる最新の注文で検品申込をしたか
        - **前回の検品対象商品注文で検品申込あり**: 検品対象商品が含まれる前回の注文で検品申込をしたか
        - **検品離脱**: 前回は検品申込があったが、最新の検品対象商品注文では申込がなかった
        """)


def create_basic_metrics(df: pd.DataFrame, order_stats: Dict[str, Any]) -> None:
    color_palette = [
        "#6366f1",
        "#a5b4fc",
        "#22c55e",
        "#86efac",
    ]

    [tab_order] = st.tabs(["注文情報"])

    with tab_order:
        # メトリクス表示部分（既存のコード）
        row1_col1, row1_col2 = st.columns(2)
        with row1_col1:
            st.metric(
                "総注文数",
                f"{order_stats['total_orders']:,}件",
                help="分析期間内に受注した全注文の総数",
            )
        with row1_col2:
            st.metric(
                "会員数",
                f"{order_stats['total_customers']:,}人",
                help="分析期間内に注文をした会員のユニーク人数",
            )

        row2_col1, row2_col2 = st.columns(2)
        with row2_col1:
            st.metric(
                "検品対象商品購入者数",
                f"{order_stats['eligible_customers']:,}人",
                help="期間内に検品対象商品を購入した顧客の総数",
            )
        with row2_col2:
            st.metric(
                "リクエスト検品申込率",
                f"{order_stats['inspection_rate']:.1f}% ({order_stats['inspection_customers']:,}/{order_stats['total_customers']:,}人)",
                help="全顧客数に対する検品申込のあった顧客の割合",
            )

        st.markdown("#### 会員分析")
        row3_col1, row3_col2 = st.columns(2)

        member_metrics = order_stats["member_metrics"]
        with row3_col1:
            member_inspection_rate = (
                member_metrics["inspection_customers"]
                / member_metrics["eligible_customers"]
                * 100
                if member_metrics["eligible_customers"] > 0
                else 0.0
            )
            st.metric(
                "会員の検品申込率",
                f"{member_inspection_rate:.1f}% ({member_metrics['inspection_customers']:,}/{member_metrics['eligible_customers']:,}人)",
                help="検品対象商品を購入した会員のうち、実際に検品を申し込んだ割合",
            )

        with row3_col2:
            member_eligible_rate = (
                member_metrics["eligible_customers"]
                / member_metrics["total_customers"]
                * 100
                if member_metrics["total_customers"] > 0
                else 0.0
            )
            st.metric(
                "会員の検品対象商品購入率",
                f"{member_eligible_rate:.1f}% ({member_metrics['eligible_customers']:,}/{member_metrics['total_customers']:,}人)",
                help="全会員のうち、検品対象商品を購入した割合",
            )

        # 1. 会員状態別の顧客構成（ドーナツチャート）
        chart_col1, chart_col2 = st.columns(2)

        with chart_col1:
            member_composition = {
                "names": ["会員", "ゲスト"],
                "values": [
                    order_stats["member_metrics"]["total_customers"],
                    order_stats["guest_metrics"]["total_customers"],
                ],
            }

            fig_member_composition = go.Figure(
                data=[
                    go.Pie(
                        labels=member_composition["names"],
                        values=member_composition["values"],
                        hole=0.4,
                        marker_colors=[color_palette[0], color_palette[1]],
                    )
                ]
            )

            fig_member_composition.update_layout(
                title={
                    "text": "会員状態別の顧客構成",
                    "y": 0.95,
                    "x": 0.5,
                    "xanchor": "center",
                    "yanchor": "top",
                },
                showlegend=True,
                legend={"orientation": "h", "yanchor": "bottom", "y": -0.2},
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
            )

            st.plotly_chart(fig_member_composition, use_container_width=True)

        # 2. 検品関連の指標比較（横棒グラフ）
        with chart_col2:
            inspection_metrics = {
                "metrics": ["会員の検品申込率", "会員の検品対象商品購入率"],
                "values": [member_inspection_rate, member_eligible_rate],
            }

            fig_inspection_rates = go.Figure(
                data=[
                    go.Bar(
                        x=inspection_metrics["values"],
                        y=inspection_metrics["metrics"],
                        orientation="h",
                        marker_color=[color_palette[0], color_palette[2]],
                    )
                ]
            )

            fig_inspection_rates.update_layout(
                title={
                    "text": "会員のリクエスト検品関連指標",
                    "y": 0.95,
                    "x": 0.5,
                    "xanchor": "center",
                    "yanchor": "top",
                },
                xaxis_title="割合(%)",
                yaxis={"categoryorder": "total ascending"},
                showlegend=False,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
            )

            fig_inspection_rates.update_traces(
                texttemplate="%{x:.1f}%", textposition="outside"
            )

            st.plotly_chart(fig_inspection_rates, use_container_width=True)

        # 3. 検品利用状況（積み上げ横棒グラフ）
        st.markdown("#### 検品利用状況")

        member_data = {
            "検品申込あり": order_stats["member_metrics"]["inspection_customers"],
            "検品申込なし": (
                order_stats["member_metrics"]["eligible_customers"]
                - order_stats["member_metrics"]["inspection_customers"]
            ),
        }

        guest_data = {
            "検品申込あり": order_stats["guest_metrics"]["inspection_customers"],
            "検品申込なし": (
                order_stats["guest_metrics"]["eligible_customers"]
                - order_stats["guest_metrics"]["inspection_customers"]
            ),
        }

        fig_inspection_status = go.Figure()

        # 会員の検品状況
        fig_inspection_status.add_trace(
            go.Bar(
                name="検品申込あり",
                y=["会員"],
                x=[member_data["検品申込あり"]],
                orientation="h",
                marker_color=color_palette[0],
                text=f"{member_data['検品申込あり']:,}人",
                textposition="auto",
            )
        )

        fig_inspection_status.add_trace(
            go.Bar(
                name="検品申込なし",
                y=["会員"],
                x=[member_data["検品申込なし"]],
                orientation="h",
                marker_color=color_palette[1],
                text=f"{member_data['検品申込なし']:,}人",
                textposition="auto",
            )
        )

        # ゲストの検品状況
        fig_inspection_status.add_trace(
            go.Bar(
                name="検品申込あり",
                y=["ゲスト"],
                x=[guest_data["検品申込あり"]],
                orientation="h",
                marker_color=color_palette[0],
                text=f"{guest_data['検品申込あり']:,}人",
                textposition="auto",
                showlegend=False,
            )
        )

        fig_inspection_status.add_trace(
            go.Bar(
                name="検品申込なし",
                y=["ゲスト"],
                x=[guest_data["検品申込なし"]],
                orientation="h",
                marker_color=color_palette[1],
                text=f"{guest_data['検品申込なし']:,}人",
                textposition="auto",
                showlegend=False,
            )
        )

        fig_inspection_status.update_layout(
            title={
                "text": "会員状態別の検品利用状況",
                "y": 0.95,
                "x": 0.5,
                "xanchor": "center",
                "yanchor": "top",
            },
            barmode="stack",
            showlegend=True,
            legend={
                "orientation": "h",
                "yanchor": "bottom",
                "y": -0.2,
                "xanchor": "center",
                "x": 0.5,
            },
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )

        st.plotly_chart(fig_inspection_status, use_container_width=True)

    style_metric_cards()


def create_inspection_analysis(
    df: pd.DataFrame,
    order_stats: Dict[str, Any],
    continuity_stats: Optional[Dict[str, Any]] = None,
) -> None:
    """
    検品申込の分析を行い、結果を表示する
    """
    # タブの作成
    tab_summary, tab_member = st.tabs(["総合サマリー", "会員分析"])

    # メトリクス計算用のヘルパー関数
    def safe_division(numerator: int, denominator: int, scale: float = 100.0) -> float:
        return (numerator / denominator * scale) if denominator > 0 else 0.0

    # 総合サマリータブ
    with tab_summary:
        st.subheader("リクエスト検品申込の全体概要")

        # メトリクスの表示
        col1, col2 = st.columns(2)
        with col1:
            st.metric(
                "検品対象商品購入者数",
                f"{order_stats['eligible_customers']:,}人",
                help="期間内に検品対象商品を購入した顧客の総数",
            )

        with col2:
            inspection_rate = safe_division(
                order_stats["inspection_customers"], order_stats["eligible_customers"]
            )
            st.metric(
                "検品申込率",
                f"{inspection_rate:.1f}%",
                help="検品対象商品購入者のうち、実際に検品を申し込んだ顧客の割合",
            )

        if continuity_stats:
            st.metric(
                "検品サービス継続率",
                f"{continuity_stats['continuation_rate']:.1f}%",
                help=f"検品対象商品購入者{continuity_stats['purchased_in_period']:,}人のうち、"
                f"実際に検品サービスを利用した{continuity_stats['continued_in_period']:,}人の割合",
            )

        # 会員・非会員の指標
        st.markdown("#### 会員状態別の内訳")
        member_metrics = order_stats["member_metrics"]
        guest_metrics = order_stats["guest_metrics"]

        member_data = {
            "区分": ["有効な会員", "ゲスト"],
            "総顧客数": [
                member_metrics["total_customers"],
                guest_metrics["total_customers"],
            ],
            "検品対象商品購入者数": [
                member_metrics["eligible_customers"],
                guest_metrics["eligible_customers"],
            ],
            "検品申込者数": [
                member_metrics["inspection_customers"],
                guest_metrics["inspection_customers"],
            ],
        }

        st.dataframe(
            pd.DataFrame(member_data).style.set_properties(**{"text-align": "center"})
        )

    # 会員分析タブ
    with tab_member:
        st.subheader("会員の検品申込分析")

        col1, col2 = st.columns(2)
        with col1:
            member_inspection_rate = safe_division(
                member_metrics["inspection_customers"],
                member_metrics["eligible_customers"],
            )
            st.metric(
                "会員の検品申込率",
                f"{member_inspection_rate:.1f}% ({member_metrics['inspection_customers']:,}/{member_metrics['eligible_customers']:,}人)",
                help="検品対象商品を購入した会員のうち、実際に検品を申し込んだ割合",
            )

        with col2:
            member_eligible_rate = safe_division(
                member_metrics["eligible_customers"], member_metrics["total_customers"]
            )
            st.metric(
                "会員の検品対象商品購入率",
                f"{member_eligible_rate:.1f}% ({member_metrics['eligible_customers']:,}/{member_metrics['total_customers']:,}人)",
                help="全会員のうち、検品対象商品を購入した割合",
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

    # analyzerの存在チェックを修正
    if st.session_state.analyzer is not None:
        try:
            # start_dateとend_dateを渡さないように修正
            df = st.session_state.analyzer.get_customer_inspection_stats()
            st.session_state.df = df
        except Exception as e:
            st.error(f"データの取得に失敗しました: {str(e)}")
            return

    with st.sidebar:
        st.title("リクエスト検品分析")

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

                st.session_state.continuity_stats = (
                    st.session_state.analyzer.get_inspection_continuity(
                        start_date=start_datetime, end_date=end_datetime
                    )
                )

                # 顧客別統計データの取得を追加
                st.session_state.customer_stats = (
                    st.session_state.analyzer.get_customer_inspection_stats()
                )

                # 期間指定のある分析データの取得
                if start_date and end_date:
                    # 注文統計
                    st.session_state.order_stats = (
                        st.session_state.analyzer.get_order_statistics(
                            start_date=start_date, end_date=end_date
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

        tab_basic, tab_customer_stats, tab_category = st.tabs(
            [
                "基本情報",
                "顧客別統計",
                "カテゴリー分析",
            ]
        )

        # タブの中身の表示
        with tab_basic:
            create_basic_metrics(st.session_state.df, st.session_state.order_stats)

        with tab_customer_stats:
            if "customer_stats" in st.session_state:
                create_customer_inspection_stats(st.session_state.customer_stats)
            else:
                st.info("顧客別統計データが利用できません。データを更新してください。")

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


if __name__ == "__main__":
    run_streamlit_dashboard()
