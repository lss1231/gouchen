#!/usr/bin/env python3
"""Generate mock ODS data for all Doris tables based on workspace/dbgen/schema.json.

Data scale assumptions (2024-04-16 ~ 2026-04-16, 730 days):
- Total users: 100,000
- Daily active users: 20,000
- Total orders: ~475,000 (capped under 500k)
- Products: 3,000
- Warehouses: 15
- Suppliers: 80
- SaaS paying customers: 20,000 (out of 100k users)
- App events: 100,000 (artificially capped)
"""

import json
import os
import sys
import random
import math
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pymysql
from src.config import get_settings

try:
    from faker import Faker
    fake = Faker('zh_CN')
except ImportError:
    fake = None

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'schema.json')
START_DATE = datetime(2024, 4, 16)
END_DATE = datetime(2026, 4, 16)
ETL_TIME = END_DATE.strftime('%Y-%m-%d %H:%M:%S')

# Province-city mapping (simplified: 34 provinces, each with 1~3 representative cities)
PROVINCES = list(range(1, 35))  # 34 provincial-level divisions
PROVINCE_CITIES: Dict[int, List[int]] = {p: [p * 10 + i for i in range(1, random.randint(2, 4))] for p in PROVINCES}


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


def batch_insert(cursor, table: str, columns: List[str], rows: List[Tuple], batch_size: int = 5000):
    if not rows:
        return
    col_sql = ', '.join([f'`{c}`' for c in columns])
    ph = ', '.join(['%s'] * len(columns))
    sql = f"INSERT INTO `{table}` ({col_sql}) VALUES ({ph})"
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        cursor.executemany(sql, batch)


def random_date(start: datetime, end: datetime) -> datetime:
    if start > end:
        start, end = end, start
    delta = end - start
    if delta.total_seconds() <= 0:
        return start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def random_time_on_day(day: datetime) -> datetime:
    return day + timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )


def weighted_choice(choices: List[Tuple[Any, float]]) -> Any:
    total = sum(w for _, w in choices)
    r = random.uniform(0, total)
    upto = 0.0
    for item, w in choices:
        upto += w
        if upto >= r:
            return item
    return choices[-1][0]


def generate_daily_counts(total_target: int, start: datetime, end: datetime, min_val: int, max_val: int) -> Dict[datetime, int]:
    """Generate daily counts that grow roughly linearly from min_val to max_val."""
    days = []
    d = start
    while d <= end:
        days.append(d)
        d += timedelta(days=1)
    n = len(days)
    weights = [min_val + (max_val - min_val) * (i / max(1, n - 1)) for i in range(n)]
    total_weight = sum(weights)
    counts = {day: max(0, int(total_target * w / total_weight)) for day, w in zip(days, weights)}
    # Adjust rounding error
    diff = total_target - sum(counts.values())
    if diff > 0:
        for day in sorted(days, key=lambda x: counts[x], reverse=True)[:diff]:
            counts[day] += 1
    elif diff < 0:
        for day in sorted(days, key=lambda x: counts[x])[:abs(diff)]:
            counts[day] -= 1
    return counts


def truncate_all_ods_tables(cursor):
    """Truncate all ODS tables before loading fresh data."""
    ods_tables = [
        'ods_users', 'ods_products', 'ods_orders', 'ods_order_items',
        'ods_inventory', 'ods_warehouses', 'ods_purchase_orders', 'ods_suppliers',
        'ods_subscriptions', 'ods_payments', 'ods_tickets', 'ods_app_events',
    ]
    for t in ods_tables:
        cursor.execute(f"TRUNCATE TABLE `{t}`;")
        print(f"  TRUNCATE {t}")


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------

def gen_ods_users(target: int = 100_000) -> Tuple[List[str], List[Tuple]]:
    """Generate ODS user data."""
    cols = [
        'user_id', 'username', 'email', 'phone', 'gender', 'birthday',
        'register_time', 'last_login_time', 'user_status', 'city_id', 'province_id', 'etl_time',
    ]
    # Registration grows from ~50/day to ~500/day
    daily_counts = generate_daily_counts(target, START_DATE, END_DATE, 50, 500)
    rows = []
    user_id = 1
    for day, count in sorted(daily_counts.items()):
        for _ in range(count):
            gender = weighted_choice([(0, 45), (1, 55)])
            birthday = random_date(datetime(1976, 1, 1), datetime(2008, 12, 31)).strftime('%Y-%m-%d')
            register_time = random_time_on_day(day)
            # 80% users logged in within last 30 days
            if random.random() < 0.8:
                last_login = random_date(max(register_time, END_DATE - timedelta(days=30)), END_DATE)
            else:
                last_login = random_date(register_time, END_DATE)
            province_id = random.choice(PROVINCES)
            city_id = random.choice(PROVINCE_CITIES[province_id])
            status = weighted_choice([(1, 90), (0, 10)])
            username = fake.name() if fake else f"user_{user_id}"
            email = fake.email() if fake else f"user_{user_id}@example.com"
            phone = fake.phone_number() if fake else f"138{user_id:08d}"
            rows.append((
                user_id, username, email, phone, gender, birthday,
                register_time.strftime('%Y-%m-%d %H:%M:%S'),
                last_login.strftime('%Y-%m-%d %H:%M:%S'),
                status, city_id, province_id, ETL_TIME,
            ))
            user_id += 1
    return cols, rows


def gen_ods_products(target: int = 3000) -> Tuple[List[str], List[Tuple]]:
    cols = [
        'product_id', 'product_name', 'category_id', 'brand_id', 'price', 'cost',
        'stock', 'status', 'create_time', 'update_time', 'etl_time',
    ]
    rows = []
    for pid in range(1, target + 1):
        price = round(max(9.9, random.gauss(150, 80)), 2)
        cost = round(price * random.uniform(0.4, 0.8), 2)
        stock = random.randint(0, 10000)
        status = weighted_choice([(1, 85), (0, 15)])
        create_time = random_date(START_DATE - timedelta(days=365), END_DATE)
        update_time = random_date(create_time, END_DATE)
        name = fake.word() if fake else f"商品_{pid}"
        rows.append((
            pid, name, random.randint(1, 30), random.randint(1, 100),
            price, cost, stock, status,
            create_time.strftime('%Y-%m-%d %H:%M:%S'),
            update_time.strftime('%Y-%m-%d %H:%M:%S'),
            ETL_TIME,
        ))
    return cols, rows


def gen_ods_orders(users: List[Tuple], target: int = 475_000) -> Tuple[List[str], List[Tuple]]:
    """Generate orders with user_id constrained by register_time."""
    cols = [
        'order_id', 'user_id', 'order_status', 'total_amount', 'discount_amount',
        'pay_amount', 'pay_type', 'pay_time', 'ship_time', 'receive_time',
        'create_time', 'update_time', 'address_id', 'remark', 'etl_time',
    ]
    # Daily orders grow from 250 to 1300
    daily_counts = generate_daily_counts(target, START_DATE, END_DATE, 250, 1300)
    # Index users by register_time for efficient sampling
    user_regs = {u[0]: datetime.strptime(u[6], '%Y-%m-%d %H:%M:%S') for u in users}
    all_uids = list(user_regs.keys())
    all_uids.sort()

    rows = []
    order_id = 1_000_001
    for day, count in sorted(daily_counts.items()):
        day_end = day + timedelta(days=1)
        eligible = [uid for uid in all_uids if user_regs[uid] < day_end]
        if not eligible:
            eligible = all_uids
        for _ in range(count):
            user_id = random.choice(eligible)
            status = weighted_choice([
                (1, 5), (2, 15), (3, 20), (4, 55), (5, 5)
            ])
            total = round(max(10, random.gauss(180, 60)), 2)
            discount = round(total * random.uniform(0, 0.25), 2)
            pay = round(total - discount, 2)
            pay_type = weighted_choice([(1, 40), (2, 40), (3, 20)])
            create_time = random_time_on_day(day)
            pay_time = ship_time = receive_time = None
            update_time = create_time
            if status in (2, 3, 4):
                pay_time = create_time + timedelta(minutes=random.randint(0, 30))
                update_time = pay_time
            if status in (3, 4):
                ship_time = pay_time + timedelta(hours=random.randint(2, 48))
                update_time = ship_time
            if status == 4:
                receive_time = ship_time + timedelta(hours=random.randint(24, 120))
                update_time = receive_time
            if status == 5:
                update_time = create_time + timedelta(hours=random.randint(1, 24))

            remark = fake.sentence() if fake and random.random() < 0.2 else ''
            rows.append((
                order_id, user_id, status, total, discount, pay, pay_type,
                pay_time.strftime('%Y-%m-%d %H:%M:%S') if pay_time else None,
                ship_time.strftime('%Y-%m-%d %H:%M:%S') if ship_time else None,
                receive_time.strftime('%Y-%m-%d %H:%M:%S') if receive_time else None,
                create_time.strftime('%Y-%m-%d %H:%M:%S'),
                update_time.strftime('%Y-%m-%d %H:%M:%S'),
                random.randint(1, 500_000), remark, ETL_TIME,
            ))
            order_id += 1
    return cols, rows


def gen_ods_order_items(orders: List[Tuple], products: List[Tuple]) -> Tuple[List[str], List[Tuple]]:
    cols = ['item_id', 'order_id', 'product_id', 'product_name', 'quantity', 'unit_price', 'total_amount']
    # Filter active products
    active_products = [p for p in products if p[7] == 1]
    product_map = {p[0]: p for p in active_products}
    product_ids = list(product_map.keys())
    rows = []
    item_id = 1
    for order in orders:
        order_id = order[0]
        n_items = weighted_choice([(1, 70), (2, 25), (3, 4), (4, 1)])
        chosen_pids = random.sample(product_ids, min(n_items, len(product_ids)))
        for pid in chosen_pids:
            prod = product_map[pid]
            qty = weighted_choice([(1, 70), (2, 20), (3, 8), (4, 2)])
            unit_price = round(prod[4] * random.uniform(0.9, 1.1), 2)
            total = round(qty * unit_price, 2)
            rows.append((item_id, order_id, pid, prod[1], qty, unit_price, total))
            item_id += 1
    return cols, rows


def gen_ods_warehouses(target: int = 15) -> Tuple[List[str], List[Tuple]]:
    cols = ['warehouse_id', 'warehouse_name', 'warehouse_type', 'province_id', 'city_id', 'capacity', 'manager_name', 'status', 'etl_time']
    types = ['central'] * 3 + ['region'] * 8 + ['front'] * 4
    names_pool = ['华北中心仓', '华东中心仓', '华南中心仓',
                  '北京仓', '上海仓', '广州仓', '深圳仓', '杭州仓', '南京仓', '成都仓', '武汉仓', '西安仓',
                  '朝阳前置仓', '浦东前置仓', '天河前置仓']
    rows = []
    for wid in range(1, target + 1):
        province_id = random.choice(PROVINCES)
        city_id = random.choice(PROVINCE_CITIES[province_id])
        name = names_pool[wid - 1] if wid <= len(names_pool) else f"仓库_{wid}"
        rows.append((
            wid, name, types[wid - 1], province_id, city_id,
            random.randint(100_000, 5_000_000),
            fake.name() if fake else f"经理_{wid}",
            weighted_choice([(1, 90), (0, 10)]),
            ETL_TIME,
        ))
    return cols, rows


def gen_ods_suppliers(target: int = 80) -> Tuple[List[str], List[Tuple]]:
    cols = ['supplier_id', 'supplier_name', 'supplier_level', 'cooperation_status', 'payment_terms', 'province_id', 'etl_time']
    rows = []
    for sid in range(1, target + 1):
        province_id = random.choice(PROVINCES)
        name = fake.company() if fake else f"供应商_{sid}"
        rows.append((
            sid, name,
            weighted_choice([('A', 20), ('B', 40), ('C', 30), ('D', 10)]),
            weighted_choice([(1, 70), (0, 20), (2, 10)]),
            weighted_choice([(15, 10), (30, 30), (45, 25), (60, 20), (90, 15)]),
            province_id, ETL_TIME,
        ))
    return cols, rows


def gen_ods_inventory(warehouses: List[Tuple], products: List[Tuple]) -> Tuple[List[str], List[Tuple]]:
    cols = ['inventory_id', 'warehouse_id', 'product_id', 'quantity', 'available_qty', 'reserved_qty', 'in_transit_qty', 'update_time', 'etl_time']
    wids = [w[0] for w in warehouses if w[7] == 1]  # active warehouses
    pids = [p[0] for p in products if p[7] == 1]     # active products
    rows = []
    inv_id = 1
    for wid in wids:
        # each warehouse covers ~1500-2200 SKUs
        n_skus = random.randint(1500, min(2200, len(pids)))
        chosen = random.sample(pids, n_skus)
        for pid in chosen:
            qty = random.randint(100, 5000)
            avail = int(qty * random.uniform(0.6, 0.95))
            reserved = int(qty * random.uniform(0, 0.2))
            transit = int(qty * random.uniform(0, 0.15))
            update_time = random_date(END_DATE - timedelta(days=30), END_DATE)
            rows.append((inv_id, wid, pid, qty, avail, reserved, transit,
                         update_time.strftime('%Y-%m-%d %H:%M:%S'), ETL_TIME))
            inv_id += 1
    return cols, rows


def gen_ods_purchase_orders(suppliers: List[Tuple], warehouses: List[Tuple], target: int = 35_000) -> Tuple[List[str], List[Tuple]]:
    cols = ['po_id', 'supplier_id', 'warehouse_id', 'po_date', 'total_amount', 'status', 'delivery_date', 'create_time', 'etl_time']
    active_sids = [s[0] for s in suppliers if s[3] == 1]
    active_wids = [w[0] for w in warehouses if w[7] == 1]
    # monthly counts grow with supplier activity
    months = []
    m = START_DATE.replace(day=1)
    while m <= END_DATE:
        months.append(m)
        if m.month == 12:
            m = m.replace(year=m.year + 1, month=1)
        else:
            m = m.replace(month=m.month + 1)
    n_months = len(months)
    # weights: early 2/order/supplier, late 6/order/supplier
    avg_monthly = [2 + (6 - 2) * (i / max(1, n_months - 1)) for i in range(n_months)]
    counts = [max(1, int(len(active_sids) * rate)) for rate in avg_monthly]
    diff = target - sum(counts)
    if diff > 0:
        for i in sorted(range(n_months), key=lambda i: counts[i], reverse=True)[:diff]:
            counts[i] += 1
    elif diff < 0:
        for i in sorted(range(n_months), key=lambda i: counts[i])[:abs(diff)]:
            counts[i] -= 1

    rows = []
    po_id = 1
    for month, count in zip(months, counts):
        for _ in range(count):
            sid = random.choice(active_sids)
            wid = random.choice(active_wids)
            po_date = random_date(month, min(month + timedelta(days=31), END_DATE) - timedelta(days=1))
            total = round(random.uniform(5000, 500000), 2)
            status = weighted_choice([(3, 60), (2, 20), (1, 15), (4, 5)])
            delivery_date = (po_date + timedelta(days=random.randint(3, 15))).strftime('%Y-%m-%d')
            create_time = random_time_on_day(po_date)
            rows.append((
                po_id, sid, wid, po_date.strftime('%Y-%m-%d'),
                total, status, delivery_date,
                create_time.strftime('%Y-%m-%d %H:%M:%S'), ETL_TIME,
            ))
            po_id += 1
    return cols, rows


def gen_ods_subscriptions(users: List[Tuple], target: int = 100_000) -> Tuple[List[str], List[Tuple]]:
    cols = [
        'subscription_id', 'customer_id', 'plan_id', 'status', 'mrr',
        'start_date', 'end_date', 'channel_id', 'create_time', 'etl_time',
    ]
    # Choose ~20k SaaS customers from 100k users
    all_uids = [u[0] for u in users]
    saas_uids = sorted(random.sample(all_uids, 20_000))
    user_reg = {u[0]: datetime.strptime(u[6], '%Y-%m-%d %H:%M:%S') for u in users}
    plan_mrr = {1: 0, 2: 99, 3: 299, 4: 999, 5: 4999}
    rows = []
    sub_id = 1
    for cid in saas_uids:
        n_subs = weighted_choice([(3, 20), (4, 35), (5, 30), (6, 15)])
        current = user_reg[cid] + timedelta(days=random.randint(0, 30))
        for _ in range(n_subs):
            plan_id = weighted_choice([(1, 10), (2, 35), (3, 35), (4, 15), (5, 5)])
            mrr = round(plan_mrr[plan_id] * random.uniform(0.9, 1.1), 2)
            status = weighted_choice([(1, 55), (2, 25), (3, 10), (4, 10)])
            start_date = current.date()
            if status == 1:
                # active: end_date far in future or None
                end_date = (END_DATE + timedelta(days=random.randint(30, 365))).date()
            elif status == 3:
                end_date = (current + timedelta(days=random.randint(30, 180))).date()
            else:
                end_date = (current + timedelta(days=random.randint(30, 365))).date()
            end_date_str = end_date.strftime('%Y-%m-%d') if end_date else None
            create_time = random_time_on_day(datetime.combine(start_date, datetime.min.time()))
            rows.append((
                sub_id, cid, plan_id, status, mrr,
                start_date.strftime('%Y-%m-%d'), end_date_str,
                random.randint(1, 8),
                create_time.strftime('%Y-%m-%d %H:%M:%S'), ETL_TIME,
            ))
            sub_id += 1
            # next subscription starts 0~90 days after previous ended
            current = datetime.combine(end_date, datetime.min.time()) + timedelta(days=random.randint(0, 90))
            if current > END_DATE + timedelta(days=90):
                break
    return cols, rows


def gen_ods_payments(subscriptions: List[Tuple], target: int = 180_000) -> Tuple[List[str], List[Tuple]]:
    cols = ['payment_id', 'customer_id', 'subscription_id', 'amount', 'currency', 'payment_date', 'payment_method', 'status', 'etl_time']
    rows = []
    pay_id = 1
    for sub in subscriptions:
        sub_id, cid, plan_id, status, mrr, start_str, end_str, *_ = sub
        start = datetime.strptime(start_str, '%Y-%m-%d')
        end = datetime.strptime(end_str, '%Y-%m-%d') if end_str else END_DATE
        if end < start:
            continue
        # 20% annual, 80% monthly
        is_annual = random.random() < 0.2
        if is_annual:
            amount = round(mrr * 12 * random.uniform(0.85, 1.0), 2)  # slight annual discount
            pay_dates = []
            d = start
            while d <= end:
                pay_dates.append(d)
                d += timedelta(days=365)
        else:
            amount = round(mrr, 2)
            pay_dates = []
            d = start.replace(day=1)
            while d <= end:
                pay_dates.append(d)
                if d.month == 12:
                    d = d.replace(year=d.year + 1, month=1)
                else:
                    d = d.replace(month=d.month + 1)
        for pd in pay_dates:
            pstatus = weighted_choice([(1, 92), (2, 5), (3, 3)])
            actual_amount = amount if pstatus != 3 else round(-amount * random.uniform(0.5, 1.0), 2)
            rows.append((
                pay_id, cid, sub_id, actual_amount,
                weighted_choice([('CNY', 90), ('USD', 10)]),
                (pd + timedelta(days=random.randint(-3, 3))).strftime('%Y-%m-%d'),
                weighted_choice([('alipay', 40), ('wechat', 35), ('bank_card', 20), ('other', 5)]),
                pstatus, ETL_TIME,
            ))
            pay_id += 1
    return cols, rows


def gen_ods_tickets(saas_customers: List[int], target: int = 60_000) -> Tuple[List[str], List[Tuple]]:
    cols = ['ticket_id', 'customer_id', 'ticket_type', 'priority', 'status', 'create_date', 'resolve_date', 'csat_score', 'etl_time']
    rows = []
    tid = 1
    # higher ARPU customers get more tickets (simulate with repeated sampling weights)
    daily_counts = generate_daily_counts(target, START_DATE, END_DATE, 50, 120)
    for day, count in sorted(daily_counts.items()):
        for _ in range(count):
            cid = random.choice(saas_customers)
            ttype = weighted_choice([
                ('technical', 40), ('billing', 20), ('sales', 15), ('general', 25)
            ])
            priority = weighted_choice([
                ('low', 30), ('medium', 45), ('high', 20), ('urgent', 5)
            ])
            status = weighted_choice([
                ('resolved', 60), ('closed', 25), ('pending', 10), ('open', 5)
            ])
            create_date = day
            resolve_date = None
            if status in ('resolved', 'closed'):
                resolve_date = create_date + timedelta(days=random.randint(1, 7))
            csat = random.randint(1, 5) if status in ('resolved', 'closed') else None
            rows.append((
                tid, cid, ttype, priority, status,
                create_date.strftime('%Y-%m-%d'),
                resolve_date.strftime('%Y-%m-%d') if resolve_date else None,
                csat, ETL_TIME,
            ))
            tid += 1
    return cols, rows


def gen_ods_app_events(users: List[Tuple], target: int = 100_000) -> Tuple[List[str], List[Tuple]]:
    cols = ['event_id', 'user_id', 'event_name', 'event_date', 'event_time', 'device_type', 'session_id', 'etl_time']
    # Concentrate in last 90 days
    event_start = END_DATE - timedelta(days=90)
    all_uids = [u[0] for u in users]
    # weight active users higher: users with recent login
    active_uids = [u[0] for u in users if datetime.strptime(u[7], '%Y-%m-%d %H:%M:%S') > END_DATE - timedelta(days=30)]
    pool = active_uids * 3 + all_uids
    rows = []
    for eid in range(1, target + 1):
        uid = random.choice(pool)
        event_name = weighted_choice([
            ('login', 30), ('page_view', 50), ('feature_use', 15), ('error', 3), ('signup', 2)
        ])
        event_date = random_date(event_start, END_DATE)
        # login events cluster around 9am and 8pm
        if event_name == 'login':
            hour = weighted_choice([(9, 30), (20, 40), (12, 15), (14, 15)])
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            event_time = event_date.replace(hour=hour, minute=minute, second=second)
        else:
            event_time = random_time_on_day(event_date)
        device = weighted_choice([
            ('ios', 30), ('android', 25), ('web', 35), ('miniapp', 10)
        ])
        session = f"sess_{random.randint(10000000, 99999999)}"
        rows.append((
            eid, uid, event_name,
            event_date.strftime('%Y-%m-%d'),
            event_time.strftime('%Y-%m-%d %H:%M:%S'),
            device, session, ETL_TIME,
        ))
    return cols, rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("  Doris ODS Mock Data Generator")
    print("=" * 60)

    conn = get_conn()
    cursor = conn.cursor()

    print("\n[1/5] Truncating ODS tables...")
    truncate_all_ods_tables(cursor)
    conn.commit()

    print("\n[2/5] Generating base data (users, products, warehouses, suppliers)...")
    user_cols, users = gen_ods_users(100_000)
    print(f"  ods_users: {len(users)} rows")
    batch_insert(cursor, 'ods_users', user_cols, users)

    prod_cols, products = gen_ods_products(3000)
    print(f"  ods_products: {len(products)} rows")
    batch_insert(cursor, 'ods_products', prod_cols, products)

    wh_cols, warehouses = gen_ods_warehouses(15)
    print(f"  ods_warehouses: {len(warehouses)} rows")
    batch_insert(cursor, 'ods_warehouses', wh_cols, warehouses)

    sup_cols, suppliers = gen_ods_suppliers(80)
    print(f"  ods_suppliers: {len(suppliers)} rows")
    batch_insert(cursor, 'ods_suppliers', sup_cols, suppliers)
    conn.commit()

    print("\n[3/5] Generating transaction data (orders, items, inventory, POs)...")
    order_cols, orders = gen_ods_orders(users, 475_000)
    print(f"  ods_orders: {len(orders)} rows")
    batch_insert(cursor, 'ods_orders', order_cols, orders)

    item_cols, items = gen_ods_order_items(orders, products)
    print(f"  ods_order_items: {len(items)} rows")
    batch_insert(cursor, 'ods_order_items', item_cols, items)

    inv_cols, inventory = gen_ods_inventory(warehouses, products)
    print(f"  ods_inventory: {len(inventory)} rows")
    batch_insert(cursor, 'ods_inventory', inv_cols, inventory)

    po_cols, pos = gen_ods_purchase_orders(suppliers, warehouses, 35_000)
    print(f"  ods_purchase_orders: {len(pos)} rows")
    batch_insert(cursor, 'ods_purchase_orders', po_cols, pos)
    conn.commit()

    print("\n[4/5] Generating SaaS data (subscriptions, payments, tickets, events)...")
    sub_cols, subs = gen_ods_subscriptions(users, 100_000)
    print(f"  ods_subscriptions: {len(subs)} rows")
    batch_insert(cursor, 'ods_subscriptions', sub_cols, subs)

    pay_cols, payments = gen_ods_payments(subs)
    print(f"  ods_payments: {len(payments)} rows")
    batch_insert(cursor, 'ods_payments', pay_cols, payments)

    saas_customers = list({s[1] for s in subs})
    ticket_cols, tickets = gen_ods_tickets(saas_customers, 60_000)
    print(f"  ods_tickets: {len(tickets)} rows")
    batch_insert(cursor, 'ods_tickets', ticket_cols, tickets)

    evt_cols, events = gen_ods_app_events(users, 100_000)
    print(f"  ods_app_events: {len(events)} rows")
    batch_insert(cursor, 'ods_app_events', evt_cols, events)
    conn.commit()

    cursor.close()
    conn.close()

    print("\n" + "=" * 60)
    print("  Done! All ODS tables populated.")
    print("=" * 60)


if __name__ == '__main__':
    main()
