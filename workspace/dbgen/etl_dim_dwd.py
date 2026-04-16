#!/usr/bin/env python3
"""ETL script for DIM and DWD layers.

Generates dimension tables and fact tables from ODS data using Doris SQL
and Python batch inserts. No day-by-day loops.
"""

import os
import sys
from datetime import datetime, timedelta
import random

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pymysql
from src.config import get_settings

START_DATE = datetime(2024, 4, 16)
END_DATE = datetime(2026, 4, 16)


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
# DIM
# ---------------------------------------------------------------------------

def gen_dim_date(cursor):
    rows = []
    d = START_DATE
    day_names = ['', '星期一', '星期二', '星期三', '星期四', '星期五', '星期六', '星期日']
    month_names = ['', '一月', '二月', '三月', '四月', '五月', '六月',
                   '七月', '八月', '九月', '十月', '十一月', '十二月']
    holidays = {
        '2025-01-01': '元旦',
        '2025-01-29': '春节', '2025-01-30': '春节', '2025-01-31': '春节',
        '2025-02-01': '春节', '2025-02-02': '春节', '2025-02-03': '春节', '2025-02-04': '春节',
        '2025-04-04': '清明', '2025-04-05': '清明', '2025-04-06': '清明',
        '2025-05-01': '劳动节', '2025-05-02': '劳动节', '2025-05-03': '劳动节',
        '2025-05-04': '劳动节', '2025-05-05': '劳动节',
        '2025-06-02': '端午', '2025-06-03': '端午',
        '2025-10-01': '国庆', '2025-10-02': '国庆', '2025-10-03': '国庆',
        '2025-10-04': '国庆', '2025-10-05': '国庆', '2025-10-06': '国庆', '2025-10-07': '国庆',
        '2026-01-01': '元旦',
        '2026-02-17': '春节', '2026-02-18': '春节', '2026-02-19': '春节',
        '2026-02-20': '春节', '2026-02-21': '春节', '2026-02-22': '春节', '2026-02-23': '春节',
        '2026-04-04': '清明', '2026-04-05': '清明', '2026-04-06': '清明',
    }
    while d <= END_DATE:
        ds = d.strftime('%Y-%m-%d')
        dw = d.isoweekday()
        holiday_name = holidays.get(ds, '')
        is_holiday = 1 if holiday_name else 0
        is_weekend = 1 if dw in (6, 7) else 0
        rows.append((
            int(d.strftime('%Y%m%d')), ds, dw, day_names[dw],
            month_names[d.month], holiday_name, d.year, d.month, d.day,
            (d.month - 1) // 3 + 1, d.isocalendar()[1], is_weekend, is_holiday
        ))
        d += timedelta(days=1)
    batch_insert(cursor, 'dim_date', [
        'date_key', 'date_str', 'day_of_week', 'day_name', 'month_name',
        'holiday_name', 'year', 'month', 'day', 'quarter', 'week_of_year',
        'is_weekend', 'is_holiday'
    ], rows)
    print(f"  dim_date: {len(rows)} rows")


def gen_dim_region(cursor):
    names = [
        '北京市','天津市','河北省','山西省','内蒙古自治区','辽宁省','吉林省','黑龙江省',
        '上海市','江苏省','浙江省','安徽省','福建省','江西省','山东省','河南省','湖北省',
        '湖南省','广东省','广西壮族自治区','海南省','重庆市','四川省','贵州省','云南省',
        '西藏自治区','陕西省','甘肃省','青海省','宁夏回族自治区','新疆维吾尔自治区',
        '台湾省','香港特别行政区','澳门特别行政区'
    ]
    codes = [
        '110000','120000','130000','140000','150000','210000','220000','230000',
        '310000','320000','330000','340000','350000','360000','370000','410000','420000',
        '430000','440000','450000','460000','500000','510000','520000','530000',
        '540000','610000','620000','630000','640000','650000',
        '710000','810000','820000'
    ]
    rows = [(i + 1, name, 1, 0, f'0/{i+1}', code) for i, (name, code) in enumerate(zip(names, codes))]
    batch_insert(cursor, 'dim_region', [
        'region_id', 'region_name', 'region_level', 'parent_id', 'region_path', 'region_code'
    ], rows)
    print(f"  dim_region: {len(rows)} rows")


def gen_dim_user_segment(cursor):
    rows = [
        (1, '新客', 'lifecycle', '注册30天内的新用户'),
        (2, '活跃用户', 'lifecycle', '近30天有登录或下单的用户'),
        (3, '沉睡用户', 'lifecycle', '30~90天未活跃的用户'),
        (4, '流失用户', 'lifecycle', '超过90天未活跃的用户'),
        (5, '高价值用户', 'value', '累计消费TOP20%的用户'),
        (6, '中价值用户', 'value', '累计消费20%~60%的用户'),
        (7, '低价值用户', 'value', '累计消费BOTTOM40%的用户'),
        (8, 'RFM重要保持', 'rfm', 'RFM模型中的重要保持客户'),
        (9, 'RFM重要挽留', 'rfm', 'RFM模型中的重要挽留客户'),
        (10, 'RFM一般客户', 'rfm', 'RFM模型中的一般客户'),
    ]
    batch_insert(cursor, 'dim_user_segment', [
        'segment_id', 'segment_name', 'segment_type', 'description'
    ], rows)
    print(f"  dim_user_segment: {len(rows)} rows")


def gen_dim_marketing_channel(cursor):
    rows = [
        (1, '抖音', 'social', 0),
        (2, '微信朋友圈', 'social', 0),
        (3, '百度搜索', 'paid', 0),
        (4, '自然搜索', 'organic', 0),
        (5, '信息流广告', 'paid', 0),
        (6, '邮件营销', 'email', 0),
        (7, 'KOL合作', 'social', 0),
        (8, '线下活动', 'organic', 0),
    ]
    batch_insert(cursor, 'dim_marketing_channel', [
        'channel_id', 'channel_name', 'channel_type', 'parent_channel_id'
    ], rows)
    print(f"  dim_marketing_channel: {len(rows)} rows")


def gen_dim_plan(cursor):
    rows = [
        (1, '免费版', 1, 0.00, 0.00, 'monthly', 1),
        (2, '基础版', 1, 99.00, 999.00, 'monthly', 1),
        (3, '专业版', 2, 299.00, 2999.00, 'monthly', 1),
        (4, '企业版', 3, 999.00, 9999.00, 'annual', 1),
        (5, '定制版', 3, 4999.00, 49999.00, 'annual', 1),
    ]
    batch_insert(cursor, 'dim_plan', [
        'plan_id', 'plan_name', 'plan_level', 'monthly_price', 'annual_price', 'billing_cycle', 'status'
    ], rows)
    print(f"  dim_plan: {len(rows)} rows")


def gen_dim_channel(cursor):
    rows = [
        (1, 'SEO', 'organic', 'fixed'),
        (2, 'SEM', 'paid', 'cpc'),
        (3, '内容营销', 'organic', 'fixed'),
        (4, '线下活动', 'organic', 'fixed'),
        (5, '客户推荐', 'referral', 'cpa'),
        (6, '代理商', 'partner', 'fixed'),
        (7, '社交媒体广告', 'paid', 'cpm'),
        (8, '产品内增长', 'organic', 'fixed'),
    ]
    batch_insert(cursor, 'dim_channel', [
        'channel_id', 'channel_name', 'channel_type', 'cost_model'
    ], rows)
    print(f"  dim_channel: {len(rows)} rows")


def gen_dim_product(cursor):
    category_map = {
        1: '数码/手机/智能手机', 2: '数码/电脑/笔记本', 3: '数码/配件/耳机',
        4: '家电/电视/4K电视', 5: '家电/冰箱/双门冰箱', 6: '家电/洗衣机/滚筒洗衣机',
        7: '服饰/男装/T恤', 8: '服饰/女装/连衣裙', 9: '服饰/童装/校服',
        10: '食品/零食/坚果', 11: '食品/饮料/牛奶', 12: '食品/生鲜/水果',
        13: '美妆/护肤/面膜', 14: '美妆/彩妆/口红', 15: '美妆/个护/洗发水',
        16: '家居/家具/沙发', 17: '家居/家纺/四件套', 18: '家居/厨具/炒锅',
        19: '运动/户外/帐篷', 20: '运动/健身/瑜伽垫', 21: '运动/鞋服/跑鞋',
        22: '母婴/奶粉/1段奶粉', 23: '母婴/玩具/积木', 24: '母婴/尿裤/纸尿裤',
        25: '图书/教育/考试用书', 26: '图书/文学/小说', 27: '图书/科技/编程',
        28: '汽车/用品/行车记录仪', 29: '汽车/保养/机油', 30: '宠物/食品/猫粮',
    }
    brand_map = {
        1: '华为', 2: '小米', 3: '苹果', 4: '联想', 5: '戴尔',
        6: '索尼', 7: '三星', 8: '海尔', 9: '美的', 10: '格力',
        11: '优衣库', 12: 'ZARA', 13: '耐克', 14: '阿迪达斯', 15: '李宁',
        16: '三只松鼠', 17: '良品铺子', 18: '伊利', 19: '蒙牛', 20: '农夫山泉',
    }
    cursor.execute(
        "SELECT product_id, product_name, category_id, brand_id, price, cost, status, create_time, update_time FROM ods_products"
    )
    rows = []
    for r in cursor.fetchall():
        pid, pname, cid, bid, price, cost, status, ct, ut = r
        cat_name = category_map.get(cid, f'类目_{cid}')
        brand_name = brand_map.get(bid, f'品牌_{bid}')
        profit = round((price - cost) / price, 4) if price else 0
        create_d = ct.strftime('%Y-%m-%d') if isinstance(ct, datetime) else str(ct)[:10]
        update_d = ut.strftime('%Y-%m-%d') if isinstance(ut, datetime) else str(ut)[:10]
        rows.append((pid, pname, cid, cat_name, bid, brand_name, price, cost, profit, status, create_d, update_d))
    batch_insert(cursor, 'dim_product', [
        'product_id', 'product_name', 'category_id', 'category_name', 'brand_id', 'brand_name',
        'price', 'cost', 'profit_rate', 'status', 'create_date', 'update_date'
    ], rows)
    print(f"  dim_product: {len(rows)} rows")


def gen_dim_warehouse(cursor):
    sql = """
    INSERT INTO dim_warehouse
    SELECT
        w.warehouse_id,
        w.warehouse_name,
        w.warehouse_type,
        COALESCE(r.region_name, CAST(w.province_id AS CHAR)) AS province_name,
        COALESCE(r.region_name, CAST(w.city_id AS CHAR)) AS city_name,
        w.capacity,
        w.status
    FROM ods_warehouses w
    LEFT JOIN dim_region r ON r.region_id = w.province_id
    """
    cursor.execute(sql)
    cursor.execute("SELECT COUNT(*) FROM dim_warehouse")
    print(f"  dim_warehouse: {cursor.fetchone()[0]} rows")


def gen_dim_supplier(cursor):
    cursor.execute(
        "SELECT supplier_id, supplier_name, supplier_level, cooperation_status FROM ods_suppliers"
    )
    rows = []
    for sid, name, level, status in cursor.fetchall():
        rating = round(random.uniform(3.0, 5.0), 2)
        if level == 'A':
            rating = min(5.0, round(rating + 0.3, 2))
        elif level == 'D':
            rating = max(3.0, round(rating - 0.3, 2))
        rows.append((sid, name, level, random.randint(1, 30), random.randint(1, 5), rating, status))
    batch_insert(cursor, 'dim_supplier', [
        'supplier_id', 'supplier_name', 'supplier_level', 'category_id',
        'cooperation_years', 'rating_score', 'status'
    ], rows)
    print(f"  dim_supplier: {len(rows)} rows")


# ---------------------------------------------------------------------------
# DWD
# ---------------------------------------------------------------------------

def gen_dwd_order_detail(cursor):
    sql = """
    INSERT INTO dwd_order_detail
    SELECT
        oi.order_id,
        oi.item_id AS order_item_id,
        o.user_id,
        oi.product_id,
        p.category_id,
        p.brand_id,
        oi.quantity,
        oi.unit_price,
        ROUND(o.discount_amount * (oi.total_amount / o.total_amount), 2) AS discount_amount,
        ROUND(oi.total_amount - o.discount_amount * (oi.total_amount / o.total_amount), 2) AS pay_amount,
        p.cost AS unit_cost,
        ROUND((oi.unit_price - p.cost) * oi.quantity, 2) AS profit,
        o.order_status,
        o.pay_type,
        u.province_id,
        u.city_id,
        DATE(o.create_time) AS order_date,
        CAST(DATE_FORMAT(o.create_time, '%Y%m') AS INT) AS order_month,
        o.pay_time,
        o.create_time
    FROM ods_order_items oi
    JOIN ods_orders o ON o.order_id = oi.order_id
    JOIN ods_users u ON u.user_id = o.user_id
    JOIN ods_products p ON p.product_id = oi.product_id
    """
    cursor.execute(sql)
    cursor.execute("SELECT COUNT(*) FROM dwd_order_detail")
    print(f"  dwd_order_detail: {cursor.fetchone()[0]} rows")


def gen_dwd_user_login(cursor):
    sql = """
    INSERT INTO dwd_user_login
    SELECT
        e.event_id AS login_id,
        e.user_id,
        DATE(e.event_time) AS login_date,
        e.event_time AS login_time,
        CASE e.device_type
            WHEN 'ios' THEN 'iOS'
            WHEN 'android' THEN 'Android'
            WHEN 'web' THEN 'PC/H5'
            WHEN 'miniapp' THEN 'H5'
            ELSE e.device_type
        END AS device_type,
        u.province_id,
        CAST(FLOOR(1 + RAND() * 8) AS INT) AS channel_id
    FROM ods_app_events e
    JOIN ods_users u ON u.user_id = e.user_id
    WHERE e.event_name = 'login'
    """
    cursor.execute(sql)
    cursor.execute("SELECT COUNT(*) FROM dwd_user_login")
    print(f"  dwd_user_login: {cursor.fetchone()[0]} rows")


def gen_dwd_marketing_event(cursor):
    cursor.execute("SELECT channel_id FROM dim_marketing_channel")
    channels = [r[0] for r in cursor.fetchall()]
    rows = []
    days = (END_DATE - START_DATE).days + 1
    evt_id = 1
    for _ in range(5000):
        d = START_DATE + timedelta(days=random.randint(0, days - 1))
        cid = random.choice(channels)
        etype = random.choices(['ad', 'promotion', 'coupon'], weights=[45, 35, 20])[0]
        cost = round(random.uniform(500, 50000), 2)
        impressions = int(cost * random.uniform(50, 200))
        clicks = int(impressions * random.uniform(0.01, 0.05))
        conversions = int(clicks * random.uniform(0.02, 0.10))
        rows.append((evt_id, d.strftime('%Y-%m-%d'), cid, etype, cost, impressions, clicks, conversions))
        evt_id += 1
    batch_insert(cursor, 'dwd_marketing_event', [
        'event_id', 'event_date', 'channel_id', 'event_type', 'cost_amount', 'impressions', 'clicks', 'conversions'
    ], rows)
    print(f"  dwd_marketing_event: {len(rows)} rows")


def gen_dwd_purchase_detail(cursor):
    cursor.execute("SELECT po_id, supplier_id, warehouse_id, po_date, total_amount, status FROM ods_purchase_orders")
    pos = cursor.fetchall()
    cursor.execute("SELECT product_id, cost FROM ods_products WHERE status = 1")
    prods = [(pid, float(cost)) for pid, cost in cursor.fetchall()]
    rows = []
    item_id = 1
    for po_id, sup_id, wh_id, po_date, total_amt, status in pos:
        n_items = random.choices([1, 2, 3], weights=[50, 35, 15])[0]
        chosen = random.sample(prods, min(n_items, len(prods)))
        item_totals = []
        for pid, cost in chosen:
            qty = random.randint(100, 5000)
            up = round(cost * random.uniform(0.8, 1.2), 2)
            item_totals.append((pid, qty, up, round(qty * up, 2)))
        current_sum = sum(x[3] for x in item_totals)
        if current_sum > 0:
            scale = float(total_amt) / current_sum
            scaled_items = [(pid, qty, round(up * scale, 2), round(qty * up * scale, 2)) for pid, qty, up, _ in item_totals]
        else:
            scaled_items = item_totals
        for pid, qty, up, tot in scaled_items:
            rows.append((po_id, item_id, sup_id, wh_id, pid, qty, up, tot))
            item_id += 1
    batch_insert(cursor, 'dwd_purchase_detail', [
        'po_id', 'po_item_id', 'supplier_id', 'warehouse_id', 'product_id', 'quantity', 'unit_price', 'total_amount'
    ], rows)
    print(f"  dwd_purchase_detail: {len(rows)} rows")


def gen_dwd_inventory_movement(cursor):
    # Preload mappings
    cursor.execute("SELECT po_id, DATE(po_date) FROM ods_purchase_orders")
    po_date_map = {r[0]: r[1] for r in cursor.fetchall()}
    cursor.execute("SELECT order_id, DATE(create_time) FROM ods_orders")
    order_date_map = {r[0]: r[1] for r in cursor.fetchall()}
    cursor.execute("SELECT warehouse_id FROM ods_warehouses WHERE status = 1")
    wh_ids = [r[0] for r in cursor.fetchall()]

    # 1. Inbound from purchase detail
    cursor.execute("SELECT po_item_id, po_id, warehouse_id, product_id, quantity FROM dwd_purchase_detail")
    purchase_items = cursor.fetchall()
    rows = []
    mov_id = 1
    for _, po_id, wh_id, pid, qty in purchase_items:
        md = po_date_map.get(po_id, START_DATE.strftime('%Y-%m-%d'))
        rows.append((mov_id, str(md), wh_id, pid, qty, 'inbound', po_id))
        mov_id += 1

    # 2. Outbound from order items
    cursor.execute("SELECT item_id, order_id, product_id, quantity FROM ods_order_items")
    for _, order_id, pid, qty in cursor.fetchall():
        wh_id = random.choice(wh_ids)
        od = order_date_map.get(order_id, '2025-01-01')
        rows.append((mov_id, str(od), wh_id, pid, -qty, 'outbound', order_id))
        mov_id += 1

    # 3. Adjust/loss based on inventory
    cursor.execute("SELECT warehouse_id, product_id, quantity FROM ods_inventory")
    for wh_id, pid, qty in cursor.fetchall():
        if random.random() < 0.1:
            adj = random.randint(-50, 50)
            if adj != 0:
                d = (END_DATE - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d')
                mtype = 'adjust' if adj > 0 else 'loss'
                rows.append((mov_id, d, wh_id, pid, adj, mtype, 0))
                mov_id += 1

    batch_insert(cursor, 'dwd_inventory_movement', [
        'movement_id', 'movement_date', 'warehouse_id', 'product_id', 'quantity', 'movement_type', 'related_order_id'
    ], rows)
    print(f"  dwd_inventory_movement: {len(rows)} rows")


def gen_dwd_subscription_events(cursor):
    cursor.execute("SELECT subscription_id, customer_id, plan_id, mrr, start_date, end_date, status FROM ods_subscriptions")
    subs = cursor.fetchall()
    cursor.execute("SELECT payment_id, subscription_id, customer_id, amount, payment_date, status FROM ods_payments")
    pay_by_sub = {}
    for pay in cursor.fetchall():
        pay_by_sub.setdefault(pay[1], []).append(pay)

    rows = []
    evt_id = 1
    for sub_id, cid, plan_id, mrr, start_date, end_date, status in subs:
        mrr_f = float(mrr)
        start_s = start_date.strftime('%Y-%m-%d') if hasattr(start_date, 'strftime') else str(start_date)
        rows.append((evt_id, start_s, cid, sub_id, 'new', plan_id, mrr_f, plan_id))
        evt_id += 1
        for pay in pay_by_sub.get(sub_id, []):
            _, _, _, amount, pdate, pstatus = pay
            amount_f = float(amount)
            pdate_s = pdate.strftime('%Y-%m-%d') if hasattr(pdate, 'strftime') else str(pdate)
            if pstatus == 1 and amount_f > 0:
                rows.append((evt_id, pdate_s, cid, sub_id, 'renewal', plan_id, amount_f, plan_id))
                evt_id += 1
        if status == 2 and end_date:
            end_s = end_date.strftime('%Y-%m-%d') if hasattr(end_date, 'strftime') else str(end_date)
            rows.append((evt_id, end_s, cid, sub_id, 'churn', plan_id, -mrr_f, plan_id))
            evt_id += 1
        if random.random() < 0.05 and end_date:
            new_plan = random.choice([x for x in [1, 2, 3, 4, 5] if x != plan_id])
            s = start_date if hasattr(start_date, 'strftime') else datetime.strptime(str(start_date), '%Y-%m-%d').date()
            e = end_date if hasattr(end_date, 'strftime') else datetime.strptime(str(end_date), '%Y-%m-%d').date()
            mid = s + timedelta(days=(e - s).days // 2)
            diff = round((new_plan - plan_id) * 100, 2)
            etype = 'upgrade' if diff > 0 else 'downgrade'
            mid_s = mid.strftime('%Y-%m-%d') if hasattr(mid, 'strftime') else str(mid)
            rows.append((evt_id, mid_s, cid, sub_id, etype, new_plan, diff, plan_id))
            evt_id += 1
    batch_insert(cursor, 'dwd_subscription_events', [
        'event_id', 'event_date', 'customer_id', 'subscription_id', 'event_type', 'plan_id', 'mrr_change', 'previous_plan_id'
    ], rows)
    print(f"  dwd_subscription_events: {len(rows)} rows")


def gen_dwd_revenue_detail(cursor):
    cursor.execute("SELECT payment_id, customer_id, subscription_id, amount, payment_date, status FROM ods_payments")
    pays = cursor.fetchall()
    cursor.execute("SELECT subscription_id, plan_id FROM ods_subscriptions")
    sub_plan = {r[0]: r[1] for r in cursor.fetchall()}
    rows = []
    rev_id = 1
    for pay_id, cid, sub_id, amount, pdate, status in pays:
        plan_id = sub_plan.get(sub_id, 1)
        amount_f = float(amount)
        pd = datetime.strptime(str(pdate), '%Y-%m-%d')
        if status == 3:  # refund
            rows.append((rev_id, pay_id, cid, sub_id, pdate, amount_f, amount_f, plan_id))
            rev_id += 1
        elif amount_f > 500:  # annual
            monthly = round(amount_f / 12, 2)
            for i in range(12):
                rd = pd + timedelta(days=30 * i)
                if rd > END_DATE:
                    break
                rows.append((rev_id, pay_id, cid, sub_id, rd.strftime('%Y-%m-%d'), amount_f, monthly, plan_id))
                rev_id += 1
        else:
            rows.append((rev_id, pay_id, cid, sub_id, pdate, amount_f, amount_f, plan_id))
            rev_id += 1
    batch_insert(cursor, 'dwd_revenue_detail', [
        'revenue_id', 'payment_id', 'customer_id', 'subscription_id', 'revenue_date', 'gross_amount', 'recognized_amount', 'plan_id'
    ], rows)
    print(f"  dwd_revenue_detail: {len(rows)} rows")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("  DIM & DWD ETL")
    print("=" * 60)
    conn = get_conn()
    cursor = conn.cursor()

    dim_tables = [
        'dim_date', 'dim_region', 'dim_product', 'dim_user_segment',
        'dim_marketing_channel', 'dim_warehouse', 'dim_supplier',
        'dim_plan', 'dim_channel',
    ]
    dwd_tables = [
        'dwd_order_detail', 'dwd_user_login', 'dwd_marketing_event',
        'dwd_inventory_movement', 'dwd_purchase_detail',
        'dwd_subscription_events', 'dwd_revenue_detail',
    ]

    print("\n[TRUNCATE DIM tables]")
    truncate_tables(cursor, dim_tables)
    print("\n[TRUNCATE DWD tables]")
    truncate_tables(cursor, dwd_tables)
    conn.commit()

    print("\n[LOAD DIM]")
    gen_dim_date(cursor)
    gen_dim_region(cursor)
    gen_dim_user_segment(cursor)
    gen_dim_marketing_channel(cursor)
    gen_dim_plan(cursor)
    gen_dim_channel(cursor)
    gen_dim_product(cursor)
    gen_dim_warehouse(cursor)
    gen_dim_supplier(cursor)
    conn.commit()

    print("\n[LOAD DWD]")
    gen_dwd_order_detail(cursor)
    gen_dwd_user_login(cursor)
    gen_dwd_marketing_event(cursor)
    gen_dwd_purchase_detail(cursor)
    gen_dwd_inventory_movement(cursor)
    gen_dwd_subscription_events(cursor)
    gen_dwd_revenue_detail(cursor)
    conn.commit()

    cursor.close()
    conn.close()
    print("\n" + "=" * 60)
    print("  Done!")
    print("=" * 60)


if __name__ == '__main__':
    main()
