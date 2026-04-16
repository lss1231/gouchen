#!/usr/bin/env python3
"""ETL script for ADS layer.

Generates application-level metric tables from DWS/ODS using Doris SQL
and Python for cohort/retention structures. No day-by-day SQL loops.
"""

import os
import sys
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pymysql
from src.config import get_settings


def get_conn():
    settings = get_settings()
    return pymysql.connect(
        host=settings.doris_host,
        port=settings.doris_port,
        user=settings.doris_user,
        password=settings.doris_password,
        database=settings.doris_database,
        charset='utf8mb4',
        autocommit=False,
    )


def batch_insert(cursor, table: str, columns: list, rows: list, batch_size: int = 5000):
    if not rows:
        return
    cols = ', '.join(f'`{c}`' for c in columns)
    ph = ', '.join(['%s'] * len(columns))
    sql = f"INSERT INTO `{table}` ({cols}) VALUES ({ph})"
    for i in range(0, len(rows), batch_size):
        cursor.executemany(sql, rows[i:i + batch_size])


def truncate_tables(cursor, tables):
    for t in tables:
        cursor.execute(f"TRUNCATE TABLE `{t}`;")
        print(f"  TRUNCATE {t}")


# ---------------------------------------------------------------------------
# SQL-based ADS tables
# ---------------------------------------------------------------------------

def gen_ads_sales_kpi(cursor):
    sql = """
    INSERT INTO ads_sales_kpi
    SELECT
      stat_date,
      'GMV' AS kpi_name,
      SUM(gmv) AS kpi_value,
      '元' AS kpi_unit,
      0.00 AS mom_growth,
      0.00 AS yoy_growth
    FROM dws_sales_daily
    GROUP BY stat_date
    UNION ALL
    SELECT
      stat_date,
      '实付金额',
      SUM(actual_amount),
      '元',
      0.00,
      0.00
    FROM dws_sales_daily
    GROUP BY stat_date
    UNION ALL
    SELECT
      stat_date,
      '利润',
      SUM(profit_amount),
      '元',
      0.00,
      0.00
    FROM dws_sales_daily
    GROUP BY stat_date
    UNION ALL
    SELECT
      stat_date,
      '订单数',
      SUM(order_count),
      '单',
      0.00,
      0.00
    FROM dws_sales_daily
    GROUP BY stat_date
    UNION ALL
    SELECT
      stat_date,
      '用户数',
      SUM(order_user_count),
      '人',
      0.00,
      0.00
    FROM dws_sales_daily
    GROUP BY stat_date
    UNION ALL
    SELECT
      a.stat_date,
      '客单价',
      ROUND(SUM(a.actual_amount) / NULLIF(SUM(a.order_count), 0), 2),
      '元',
      0.00,
      0.00
    FROM dws_sales_daily a
    GROUP BY a.stat_date
    """
    cursor.execute(sql)
    # conversion rate
    sql2 = """
    INSERT INTO ads_sales_kpi
    SELECT
      a.stat_date,
      '转化率',
      ROUND(a.paying_users / NULLIF(b.active_users, 0) * 100, 2),
      '%',
      0.00,
      0.00
    FROM dws_user_stats a
    JOIN dws_user_stats b ON b.stat_date = a.stat_date
    """
    cursor.execute(sql2)
    cursor.execute("SELECT COUNT(*) FROM ads_sales_kpi")
    print(f"  ads_sales_kpi: {cursor.fetchone()[0]} rows")


def gen_ads_region_rank(cursor):
    sql = """
    INSERT INTO ads_region_rank
    WITH monthly AS (
      SELECT
        CAST(CONCAT(stat_month, '01') AS DATE) AS stat_date,
        province_id,
        SUM(gmv) AS gmv,
        SUM(order_count) AS order_count,
        SUM(order_user_count) AS user_count
      FROM dws_sales_monthly
      GROUP BY stat_month, province_id
    ),
    total AS (
      SELECT stat_date, SUM(gmv) AS total_gmv FROM monthly GROUP BY stat_date
    )
    SELECT
      m.stat_date,
      m.province_id,
      r.region_name AS province_name,
      m.gmv,
      m.order_count,
      m.user_count,
      ROW_NUMBER() OVER (PARTITION BY m.stat_date ORDER BY m.gmv DESC) AS gmv_rank,
      ROUND(m.gmv / NULLIF(t.total_gmv, 0), 4) AS gmv_share
    FROM monthly m
    JOIN dim_region r ON r.region_id = m.province_id
    JOIN total t ON t.stat_date = m.stat_date
    """
    cursor.execute(sql)
    cursor.execute("SELECT COUNT(*) FROM ads_region_rank")
    print(f"  ads_region_rank: {cursor.fetchone()[0]} rows")


def gen_ads_category_rank(cursor):
    sql = """
    INSERT INTO ads_category_rank
    WITH monthly AS (
      SELECT
        CAST(CONCAT(stat_month, '01') AS DATE) AS stat_date,
        category_id,
        SUM(gmv) AS gmv,
        SUM(order_count) AS order_count,
        SUM(product_count) AS product_count
      FROM dws_sales_monthly
      GROUP BY stat_month, category_id
    ),
    total AS (
      SELECT stat_date, SUM(gmv) AS total_gmv FROM monthly GROUP BY stat_date
    )
    SELECT
      m.stat_date,
      m.category_id,
      p.category_name,
      m.gmv,
      m.order_count,
      m.product_count,
      ROW_NUMBER() OVER (PARTITION BY m.stat_date ORDER BY m.gmv DESC) AS gmv_rank,
      ROUND(m.gmv / NULLIF(t.total_gmv, 0), 4) AS gmv_share
    FROM monthly m
    JOIN (SELECT DISTINCT category_id, category_name FROM dim_product) p ON p.category_id = m.category_id
    JOIN total t ON t.stat_date = m.stat_date
    """
    cursor.execute(sql)
    cursor.execute("SELECT COUNT(*) FROM ads_category_rank")
    print(f"  ads_category_rank: {cursor.fetchone()[0]} rows")


def gen_ads_member_lifecycle(cursor):
    sql = """
    INSERT INTO ads_member_lifecycle
    SELECT
      CAST(CONCAT(DATE_FORMAT(stat_date, '%Y%m'), '01') AS DATE) AS stat_date,
      seg.segment_name,
      SUM(md.new_members + md.active_members) AS member_count,
      ROUND(SUM(md.active_members) / NULLIF(SUM(md.new_members + md.active_members), 0), 4) AS active_rate,
      ROUND(SUM(md.renew_members) / NULLIF(SUM(md.new_members + md.active_members), 0), 4) AS renewal_rate,
      ROUND(SUM(md.member_gmv) / NULLIF(SUM(md.new_members + md.active_members), 0), 2) AS arpu
    FROM dws_member_daily md
    JOIN dim_user_segment seg ON seg.segment_id = md.segment_id
    GROUP BY DATE_FORMAT(stat_date, '%Y%m'), seg.segment_name
    """
    cursor.execute(sql)
    cursor.execute("SELECT COUNT(*) FROM ads_member_lifecycle")
    print(f"  ads_member_lifecycle: {cursor.fetchone()[0]} rows")


def gen_ads_marketing_roi(cursor):
    sql = """
    INSERT INTO ads_marketing_roi
    SELECT
      e.event_date AS stat_date,
      c.channel_name,
      SUM(e.cost_amount) AS cost_amount,
      ROUND(SUM(e.cost_amount) * 3.5, 2) AS conversion_gmv,
      ROUND(SUM(e.cost_amount) * 3.5 / NULLIF(SUM(e.cost_amount), 0), 4) AS roi,
      ROUND(SUM(e.cost_amount) / NULLIF(SUM(e.conversions), 0), 2) AS cac,
      ROUND(SUM(e.clicks) / NULLIF(SUM(e.impressions), 0), 4) AS ctr
    FROM dwd_marketing_event e
    JOIN dim_marketing_channel c ON c.channel_id = e.channel_id
    GROUP BY e.event_date, c.channel_name
    """
    cursor.execute(sql)
    cursor.execute("SELECT COUNT(*) FROM ads_marketing_roi")
    print(f"  ads_marketing_roi: {cursor.fetchone()[0]} rows")


def gen_ads_inventory_turnover(cursor):
    sql = """
    INSERT INTO ads_inventory_turnover
    SELECT
      CAST(DATE_FORMAT(stat_date, '%Y%m') AS INT) AS stat_month,
      warehouse_id,
      product_id,
      CAST(AVG(closing_qty) AS INT) AS avg_inventory,
      ROUND(30.0 * AVG(closing_qty) / NULLIF(SUM(outbound_qty) / COUNT(*), 0), 2) AS turnover_days,
      ROUND(NULLIF(SUM(outbound_qty), 0) / NULLIF(AVG(closing_qty), 0), 4) AS turnover_rate,
      SUM(CASE WHEN closing_qty = 0 THEN 1 ELSE 0 END) AS stockout_days
    FROM dws_inventory_daily
    GROUP BY stat_month, warehouse_id, product_id
    """
    cursor.execute(sql)
    cursor.execute("SELECT COUNT(*) FROM ads_inventory_turnover")
    print(f"  ads_inventory_turnover: {cursor.fetchone()[0]} rows")


def gen_ads_stock_alert(cursor):
    sql = """
    INSERT INTO ads_stock_alert
    SELECT
      '2026-04-16' AS alert_date,
      warehouse_id,
      product_id,
      CASE
        WHEN available_qty < 200 THEN 'low_stock'
        WHEN quantity > 4000 THEN 'over_stock'
        ELSE 'low_stock'
      END AS alert_type,
      quantity AS current_qty,
      200 AS safety_stock,
      GREATEST(0, 500 - available_qty) AS suggest_replenish_qty
    FROM ods_inventory
    WHERE available_qty < 200 OR quantity > 4000
    """
    cursor.execute(sql)
    cursor.execute("SELECT COUNT(*) FROM ads_stock_alert")
    print(f"  ads_stock_alert: {cursor.fetchone()[0]} rows")


def gen_ads_mrr_kpi(cursor):
    sql = """
    INSERT INTO ads_mrr_kpi
    SELECT
      stat_date,
      'MRR' AS kpi_name,
      total_mrr AS kpi_value,
      0.00 AS mom_growth,
      '元' AS unit
    FROM dws_mrr_daily
    UNION ALL
    SELECT
      stat_date,
      'ARR',
      total_mrr * 12,
      0.00,
      '元'
    FROM dws_mrr_daily
    UNION ALL
    SELECT
      d.stat_date,
      'ChurnRate',
      ROUND(ABS(d.churn_mrr) / NULLIF(d.total_mrr + ABS(d.churn_mrr), 0), 4) * 100,
      0.00,
      '%'
    FROM dws_mrr_daily d
    UNION ALL
    SELECT
      stat_date,
      'NRR',
      ROUND((total_mrr + expansion_mrr) / NULLIF(total_mrr - churn_mrr, 0), 4) * 100,
      0.00,
      '%'
    FROM dws_mrr_daily
    """
    cursor.execute(sql)
    cursor.execute("SELECT COUNT(*) FROM ads_mrr_kpi")
    print(f"  ads_mrr_kpi: {cursor.fetchone()[0]} rows")


def gen_ads_nps_score(cursor):
    sql = """
    INSERT INTO ads_nps_score
    SELECT
      CAST(DATE_FORMAT(create_date, '%Y%m') AS INT) AS stat_month,
      SUM(CASE WHEN csat_score = 5 THEN 1 ELSE 0 END) AS promoters,
      SUM(CASE WHEN csat_score = 4 THEN 1 ELSE 0 END) AS passives,
      SUM(CASE WHEN csat_score <= 3 THEN 1 ELSE 0 END) AS detractors,
      ROUND(
        (SUM(CASE WHEN csat_score = 5 THEN 1 ELSE 0 END) - SUM(CASE WHEN csat_score <= 3 THEN 1 ELSE 0 END))
        * 100.0 / COUNT(*),
        2
      ) AS nps_score,
      COUNT(*) AS response_count
    FROM ods_tickets
    WHERE csat_score IS NOT NULL
    GROUP BY DATE_FORMAT(create_date, '%Y%m')
    """
    cursor.execute(sql)
    cursor.execute("SELECT COUNT(*) FROM ads_nps_score")
    print(f"  ads_nps_score: {cursor.fetchone()[0]} rows")


def gen_ads_saas_user_segment(cursor):
    sql = """
    INSERT INTO ads_saas_user_segment
    WITH daily AS (
      SELECT
        s.start_date AS stat_date,
        CASE
          WHEN s.plan_id = 5 THEN 'VIP'
          WHEN s.plan_id = 4 THEN '高活跃'
          WHEN s.plan_id = 3 THEN '中活跃'
          WHEN s.plan_id = 2 THEN '低活跃'
          WHEN s.plan_id = 1 THEN '免费用户'
        END AS segment_name,
        COUNT(*) AS customer_count,
        SUM(s.mrr) AS mrr_contribution,
        85.00 AS avg_health_score
      FROM ods_subscriptions s
      WHERE s.status = 1
      GROUP BY s.start_date, s.plan_id
    )
    SELECT * FROM daily
    UNION ALL
    SELECT
      DATE(e.event_date) AS stat_date,
      '流失风险' AS segment_name,
      COUNT(*) AS customer_count,
      SUM(ABS(e.mrr_change)) AS mrr_contribution,
      30.00 AS avg_health_score
    FROM dwd_subscription_events e
    WHERE e.event_type = 'churn'
    GROUP BY DATE(e.event_date)
    """
    cursor.execute(sql)
    cursor.execute("SELECT COUNT(*) FROM ads_saas_user_segment")
    print(f"  ads_saas_user_segment: {cursor.fetchone()[0]} rows")


# ---------------------------------------------------------------------------
# Python-based cohort / retention tables
# ---------------------------------------------------------------------------

def gen_ads_user_retention(cursor):
    print("  ads_user_retention: loading users and logins...")
    cursor.execute("SELECT user_id, DATE(register_time) FROM ods_users")
    users_by_reg = defaultdict(set)
    for uid, reg in cursor.fetchall():
        reg_s = reg.strftime('%Y-%m-%d') if hasattr(reg, 'strftime') else str(reg)
        users_by_reg[reg_s].add(uid)

    cursor.execute("SELECT user_id, login_date FROM dwd_user_login")
    logins_by_user = defaultdict(set)
    for uid, ld in cursor.fetchall():
        ld_s = ld.strftime('%Y-%m-%d') if hasattr(ld, 'strftime') else str(ld)
        logins_by_user[uid].add(ld_s)

    retention_days = [1, 3, 7, 30]
    rows = []
    for reg_date, uids in users_by_reg.items():
        total = len(uids)
        if total == 0:
            continue
        reg_dt = datetime.strptime(reg_date, '%Y-%m-%d')
        for rd in retention_days:
            target = (reg_dt + timedelta(days=rd)).strftime('%Y-%m-%d')
            retained = sum(1 for uid in uids if target in logins_by_user.get(uid, set()))
            rate = round(retained / total, 4)
            rows.append((target, reg_date, rd, total, retained, rate))

    batch_insert(cursor, 'ads_user_retention', [
        'stat_date', 'register_date', 'retention_day', 'new_user_count', 'retained_user_count', 'retention_rate'
    ], rows)
    print(f"  ads_user_retention: {len(rows)} rows")


def gen_ads_ltv_cohort(cursor):
    print("  ads_ltv_cohort: loading subscriptions and revenue...")
    cursor.execute("SELECT customer_id, start_date FROM ods_subscriptions")
    subs = cursor.fetchall()
    cursor.execute("SELECT customer_id, revenue_date, recognized_amount FROM dwd_revenue_detail")
    revenues = cursor.fetchall()

    cohort_customers = defaultdict(set)
    for cid, start in subs:
        cm = start.strftime('%Y%m') if hasattr(start, 'strftime') else str(start)[:6]
        cohort_customers[cm].add(cid)

    rev_group = defaultdict(lambda: defaultdict(lambda: {'customers': set(), 'revenue': 0.0}))
    for cid, rdate, amt in revenues:
        pm = rdate.strftime('%Y%m') if hasattr(rdate, 'strftime') else str(rdate)[:6]
        # Find cohort month for this customer
        for cm, cset in cohort_customers.items():
            if cid in cset:
                rev_group[cm][pm]['customers'].add(cid)
                rev_group[cm][pm]['revenue'] += float(amt)
                break

    rows = []
    for cm in sorted(rev_group.keys()):
        total_customers = len(cohort_customers[cm])
        periods = sorted(rev_group[cm].keys())
        cum_revenue = 0.0
        for pm in periods:
            info = rev_group[cm][pm]
            cum_revenue += info['revenue']
            ltv = round(cum_revenue / total_customers, 2) if total_customers else 0
            rate = round(len(info['customers']) / total_customers, 4) if total_customers else 0
            rows.append((int(cm), int(pm), len(info['customers']), round(info['revenue'], 2), ltv, rate))

    batch_insert(cursor, 'ads_ltv_cohort', [
        'cohort_month', 'period_month', 'customer_count', 'revenue', 'ltv', 'retention_rate'
    ], rows)
    print(f"  ads_ltv_cohort: {len(rows)} rows")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("  ADS ETL")
    print("=" * 60)
    conn = get_conn()
    cursor = conn.cursor()

    tables = [
        'ads_sales_kpi', 'ads_user_retention', 'ads_region_rank', 'ads_category_rank',
        'ads_member_lifecycle', 'ads_marketing_roi', 'ads_inventory_turnover',
        'ads_stock_alert', 'ads_mrr_kpi', 'ads_ltv_cohort', 'ads_nps_score',
        'ads_saas_user_segment',
    ]
    truncate_tables(cursor, tables)
    conn.commit()

    print("\n[LOAD ADS]")
    gen_ads_sales_kpi(cursor)
    gen_ads_region_rank(cursor)
    gen_ads_category_rank(cursor)
    gen_ads_marketing_roi(cursor)
    gen_ads_member_lifecycle(cursor)
    gen_ads_inventory_turnover(cursor)
    gen_ads_stock_alert(cursor)
    gen_ads_mrr_kpi(cursor)
    gen_ads_saas_user_segment(cursor)
    gen_ads_nps_score(cursor)
    gen_ads_user_retention(cursor)
    gen_ads_ltv_cohort(cursor)
    conn.commit()

    cursor.close()
    conn.close()
    print("\n" + "=" * 60)
    print("  Done!")
    print("=" * 60)


if __name__ == '__main__':
    main()
