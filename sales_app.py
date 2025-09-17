from flask import Flask, render_template, request
import psycopg2
from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import logging
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import os
from cryptography.fernet import Fernet

app = Flask(__name__)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("sales.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Загружаем переменные окружения из .env
load_dotenv()

ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
cipher = Fernet(ENCRYPTION_KEY)

# Определяем функции шифрования
def encrypt_data(data: str) -> bytes:
    return cipher.encrypt(data.encode())

def decrypt_data(encrypted_data: bytes) -> str:
    return cipher.decrypt(encrypted_data).decode()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "sslmode": "require"
}

def sanitize_log_data(data):
    if not data:
        return data
    sanitized_data = []
    for row in data:
        sanitized_row = []
        for item in row:
            if isinstance(item, str) and item not in ['Не указан', 'Без категории']:
                sanitized_row.append("***")
            else:
                sanitized_row.append(item)
        sanitized_data.append(tuple(sanitized_row))
    return sanitized_data

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к базе данных: {str(e)}")
        raise

def get_categories():
    logger.info("Запрос списка категорий товаров")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = """
            SELECT DISTINCT cg."Category"
            FROM "Category_goods" cg
            ORDER BY cg."Category";
        """
        cur.execute(query)
        result = cur.fetchall()
        logger.info(f"Получено {len(result)} категорий")
        cur.close()
        conn.close()
        return [row[0] for row in result]
    except Exception as e:
        logger.error(f"Ошибка при получении списка категорий: {str(e)}")
        return []

def get_summary_stats(start_date, end_date):
    logger.info(f"Запрос сводной статистики для {start_date} - {end_date}")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = """
            WITH 
            -- Выручка (сумма всех заказов с учетом скидок)
            revenue AS (
                SELECT COALESCE(SUM(og."Sum_and_discont_og"), 0) AS total_revenue
                FROM "Order" o
                JOIN "Order_goods" og ON o."OrderID" = og."OrderID"
                WHERE o."Date_order" BETWEEN %s AND %s
            ),
            -- Расходы на зарплаты
            salary_expenses AS (
                SELECT SUM("Salary") AS total_salary_expenses
                FROM "Staff"
            ),
            -- Расходы на аренду
            rent_expenses AS (
                SELECT COALESCE(SUM("Rental_price_month"), 0) AS total_rent_expenses
                FROM "Store"
            ),
            -- Себестоимость проданных товаров (FIFO)
            cost_of_goods_sold AS (
                SELECT COALESCE(SUM(s."Price_supply" * og."Quantity_goods"), 0) AS total_cogs
                FROM "Order" o
                JOIN "Order_goods" og ON o."OrderID" = og."OrderID"
                LEFT JOIN "Supply" s ON og."GoodID" = s."GoodsID"
                    AND s."Date_supply" = (
                        SELECT MIN(s2."Date_supply")
                        FROM "Supply" s2
                        WHERE s2."GoodsID" = s."GoodsID"
                        AND s2."Date_supply" <= o."Date_order"
                    )
                WHERE o."Date_order" BETWEEN %s AND %s
            ),
            -- Дополнительные расходы
            other_expenses AS (
                SELECT 
                    200000 AS marketing,
                    470000 AS logistics,
                    175000 AS utilities,
                    25000 AS mobile_phone,
                    60000 AS equipment_depreciation
            )
            SELECT 
                r.total_revenue AS total_revenue,
                (r.total_revenue - (r.total_revenue * 0.20) - cogs.total_cogs - se.total_salary_expenses - re.total_rent_expenses -
                 oe.marketing - oe.logistics - oe.utilities - oe.mobile_phone - oe.equipment_depreciation -
                 ((r.total_revenue - (r.total_revenue * 0.20) - cogs.total_cogs - se.total_salary_expenses - re.total_rent_expenses - 
                   oe.marketing - oe.logistics - oe.utilities - oe.mobile_phone - oe.equipment_depreciation) * 0.15)) AS net_profit,
                (cogs.total_cogs + se.total_salary_expenses + re.total_rent_expenses + 
                 oe.marketing + oe.logistics + oe.utilities + oe.mobile_phone + oe.equipment_depreciation +
                 (r.total_revenue * 0.20) + 
                 ((r.total_revenue - (r.total_revenue * 0.20) - cogs.total_cogs - se.total_salary_expenses - re.total_rent_expenses - 
                   oe.marketing - oe.logistics - oe.utilities - oe.mobile_phone - oe.equipment_depreciation) * 0.15)) AS total_expenses,
                (SELECT COUNT(DISTINCT o."OrderID") FROM "Order" o WHERE o."Date_order" BETWEEN %s AND %s) AS total_orders,
                cogs.total_cogs AS cost_of_goods_sold,
                se.total_salary_expenses AS salary_expenses,
                re.total_rent_expenses AS rent_expenses,
                oe.marketing AS marketing,
                oe.logistics AS logistics,
                oe.utilities AS utilities,
                oe.mobile_phone AS mobile_phone,
                oe.equipment_depreciation AS equipment_depreciation,
                (r.total_revenue * 0.20) AS vat,
                ((r.total_revenue - (r.total_revenue * 0.20) - cogs.total_cogs - se.total_salary_expenses - re.total_rent_expenses - 
                  oe.marketing - oe.logistics - oe.utilities - oe.mobile_phone - oe.equipment_depreciation) * 0.15) AS profit_tax
            FROM 
                revenue r,
                salary_expenses se,
                rent_expenses re,
                cost_of_goods_sold cogs,
                other_expenses oe;
        """
        cur.execute(query, (start_date, end_date, start_date, end_date, start_date, end_date))
        result = cur.fetchone()
        logger.info(f"Получены сводные данные: {sanitize_log_data([result])}")
        cur.close()
        conn.close()
        return {
            'total_revenue': result[0] or 0,
            'net_profit': result[1] or 0,
            'total_expenses': result[2] or 0,
            'total_orders': result[3] or 0,
            'cost_of_goods_sold': result[4] or 0,
            'salary_expenses': result[5] or 0,
            'rent_expenses': result[6] or 0,
            'marketing': result[7] or 0,
            'logistics': result[8] or 0,
            'utilities': result[9] or 0,
            'mobile_phone': result[10] or 0,
            'equipment_depreciation': result[11] or 0,
            'vat': result[12] or 0,
            'profit_tax': result[13] or 0
        }
    except Exception as e:
        logger.error(f"Ошибка при получении сводной статистики: {str(e)}")
        return {
            'total_revenue': 0,
            'net_profit': 0,
            'total_expenses': 0,
            'total_orders': 0,
            'cost_of_goods_sold': 0,
            'salary_expenses': 0,
            'rent_expenses': 0,
            'marketing': 0,
            'logistics': 0,
            'utilities': 0,
            'mobile_phone': 0,
            'equipment_depreciation': 0,
            'vat': 0,
            'profit_tax': 0
        }

def get_gross_profit(start_date, end_date):
    logger.info(f"Запрос валовой прибыли для {start_date} - {end_date}")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = """
            WITH 
            -- Выручка по дням
            revenue AS (
                SELECT 
                    DATE_TRUNC('day', o."Date_order") AS date,
                    COALESCE(SUM(og."Sum_and_discont_og"), 0) AS total_revenue
                FROM "Order" o
                JOIN "Order_goods" og ON o."OrderID" = og."OrderID"
                WHERE o."Date_order" BETWEEN %s AND %s
                GROUP BY DATE_TRUNC('day', o."Date_order")
            ),
            -- Себестоимость проданных товаров по дням (FIFO)
            cost_of_goods_sold AS (
                SELECT 
                    DATE_TRUNC('day', o."Date_order") AS date,
                    COALESCE(SUM(s."Price_supply" * og."Quantity_goods"), 0) AS total_cogs
                FROM "Order" o
                JOIN "Order_goods" og ON o."OrderID" = og."OrderID"
                LEFT JOIN "Supply" s ON og."GoodID" = s."GoodsID"
                    AND s."Date_supply" = (
                        SELECT MIN(s2."Date_supply")
                        FROM "Supply" s2
                        WHERE s2."GoodsID" = s."GoodsID"
                        AND s2."Date_supply" <= o."Date_order"
                    )
                WHERE o."Date_order" BETWEEN %s AND %s
                GROUP BY DATE_TRUNC('day', o."Date_order")
            ),
            -- Объединяем данные
            daily_data AS (
                SELECT 
                    r.date,
                    r.total_revenue,
                    cogs.total_cogs,
                    (r.total_revenue - cogs.total_cogs) AS gross_profit
                FROM revenue r
                LEFT JOIN cost_of_goods_sold cogs ON r.date = cogs.date
            )
            SELECT 
                date,
                total_revenue AS revenue,
                total_cogs AS purchase_cost,
                gross_profit
            FROM daily_data
            ORDER BY date;
        """
        cur.execute(query, (start_date, end_date, start_date, end_date))
        result = cur.fetchall()
        logger.info(f"Получено {len(result)} записей для валовой прибыли")
        cur.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка при получении валовой прибыли: {str(e)}")
        return []

def get_orders_count(start_date, end_date):
    logger.info(f"Запрос количества заказов для {start_date} - {end_date}")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = """
            SELECT 
                DATE_TRUNC('day', o."Date_order") AS date,
                COUNT(DISTINCT o."OrderID") AS order_count
            FROM 
                "Order" o
            JOIN 
                "Realization" r ON o."RealizationID" = r."RealizationID"
            JOIN 
                "Store" s ON r."StoreID" = s."StoreID"
            WHERE o."Date_order" BETWEEN %s AND %s
            GROUP BY 
                DATE_TRUNC('day', o."Date_order")
            ORDER BY 
                date;
        """
        cur.execute(query, (start_date, end_date))
        result = cur.fetchall()
        logger.info(f"Получено {len(result)} записей для количества заказов")
        cur.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка при получении количества заказов: {str(e)}")
        return []

def get_avg_order_value(start_date, end_date):
    logger.info(f"Запрос среднего чека для {start_date} - {end_date}")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = """
            SELECT 
                DATE_TRUNC('day', o."Date_order") AS date,
                AVG(og."Sum_og") AS avg_order_value
            FROM 
                "Order" o
            JOIN 
                "Order_goods" og ON o."OrderID" = og."OrderID"
            JOIN 
                "Realization" r ON o."RealizationID" = r."RealizationID"
            JOIN 
                "Store" s ON r."StoreID" = s."StoreID"
            WHERE o."Date_order" BETWEEN %s AND %s
            GROUP BY 
                DATE_TRUNC('day', o."Date_order")
            ORDER BY 
                date;
        """
        cur.execute(query, (start_date, end_date))
        result = cur.fetchall()
        logger.info(f"Получено {len(result)} записей для среднего чека")
        cur.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка при получении среднего чека: {str(e)}")
        return []

def get_revenue_by_store(start_date, end_date, category=None):
    logger.info(f"Запрос выручки по магазинам для {start_date} - {end_date}, категория: {category}")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = """
            SELECT 
                CASE WHEN s."StoreID" = 0 THEN 'Сайт' ELSE s."Name" END AS store_name,
                SUM(og."Sum_og") AS total_revenue
            FROM 
                "Order" o
            JOIN 
                "Realization" r ON o."RealizationID" = r."RealizationID"
            JOIN 
                "Store" s ON r."StoreID" = s."StoreID"
            JOIN 
                "Order_goods" og ON o."OrderID" = og."OrderID"
            JOIN 
                "Goods" g ON og."GoodID" = g."GoodID"
            JOIN 
                "Category_goods" cg ON g."Category_goodsID" = cg."Category_goodsID"
            WHERE o."Date_order" BETWEEN %s AND %s
        """
        params = [start_date, end_date]
        if category:
            query += ' AND cg."Category" = %s'
            params.append(category)
        query += """
            GROUP BY 
                s."StoreID", s."Name"
            ORDER BY 
                total_revenue DESC;
        """
        cur.execute(query, params)
        result = cur.fetchall()
        logger.info(f"Получено {len(result)} записей для выручки по магазинам")
        cur.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка при получении выручки по магазинам: {str(e)}")
        return []

def get_orders_by_store(start_date, end_date, category=None):
    logger.info(f"Запрос заказов по магазинам для {start_date} - {end_date}, категория: {category}")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = """
            SELECT 
                CASE WHEN s."StoreID" = 0 THEN 'Сайт' ELSE s."Name" END AS store_name,
                COUNT(DISTINCT o."OrderID") AS order_count
            FROM 
                "Order" o
            JOIN 
                "Realization" r ON o."RealizationID" = r."RealizationID"
            JOIN 
                "Store" s ON r."StoreID" = s."StoreID"
            JOIN 
                "Order_goods" og ON o."OrderID" = og."OrderID"
            JOIN 
                "Goods" g ON og."GoodID" = g."GoodID"
            JOIN 
                "Category_goods" cg ON g."Category_goodsID" = cg."Category_goodsID"
            WHERE o."Date_order" BETWEEN %s AND %s
        """
        params = [start_date, end_date]
        if category:
            query += ' AND cg."Category" = %s'
            params.append(category)
        query += """
            GROUP BY 
                s."StoreID", s."Name"
            ORDER BY 
                order_count DESC;
        """
        cur.execute(query, params)
        result = cur.fetchall()
        logger.info(f"Получено {len(result)} записей для заказов по магазинам")
        cur.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка при получении заказов по магазинам: {str(e)}")
        return []

def get_top_brands(start_date, end_date):
    logger.info(f"Запрос топ брендов для {start_date} - {end_date}")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = """
            SELECT 
                g."Brend" AS brand,
                SUM(og."Sum_and_discont_og") AS total_revenue
            FROM 
                "Order" o
            JOIN 
                "Order_goods" og ON o."OrderID" = og."OrderID"
            JOIN 
                "Goods" g ON og."GoodID" = g."GoodID"
            JOIN 
                "Realization" r ON o."RealizationID" = r."RealizationID"
            JOIN 
                "Store" s ON r."StoreID" = s."StoreID"
            WHERE o."Date_order" BETWEEN %s AND %s
            GROUP BY 
                g."Brend"
            ORDER BY 
                total_revenue DESC
            LIMIT 10;
        """
        cur.execute(query, (start_date, end_date))
        result = cur.fetchall()
        logger.info(f"Получено {len(result)} записей для топ брендов")
        cur.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка при получении топ брендов: {str(e)}")
        return []

def get_top_categories(start_date, end_date):
    logger.info(f"Запрос топ категорий для {start_date} - {end_date}")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = """
            SELECT 
                cg."Category" AS category,
                SUM(og."Sum_and_discont_og") AS total_revenue
            FROM 
                "Order" o
            JOIN 
                "Order_goods" og ON o."OrderID" = og."OrderID"
            JOIN 
                "Goods" g ON og."GoodID" = g."GoodID"
            JOIN 
                "Category_goods" cg ON g."Category_goodsID" = cg."Category_goodsID"
            JOIN 
                "Realization" r ON o."RealizationID" = r."RealizationID"
            JOIN 
                "Store" s ON r."StoreID" = s."StoreID"
            WHERE o."Date_order" BETWEEN %s AND %s
            GROUP BY 
                cg."Category"
            ORDER BY 
                total_revenue DESC
            LIMIT 10;
        """
        cur.execute(query, (start_date, end_date))
        result = cur.fetchall()
        logger.info(f"Получено {len(result)} записей для топ категорий")
        cur.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка при получении топ категорий: {str(e)}")
        return []

def get_sales_by_manager(start_date, end_date):
    logger.info(f"Запрос продаж по менеджерам для {start_date} - {end_date}")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = """
            SELECT 
                m."Last_name" || ' ' || m."First_name" AS manager_name,
                SUM(og."Sum_and_discont_og") AS total_revenue
            FROM 
                "Order" o
            JOIN 
                "Realization" r ON o."RealizationID" = r."RealizationID"
            JOIN 
                "Staff" m ON r."StaffID" = m."StaffID"
            JOIN 
                "Order_goods" og ON o."OrderID" = og."OrderID"
            JOIN 
                "Store" s ON r."StoreID" = s."StoreID"
            WHERE o."Date_order" BETWEEN %s AND %s
            GROUP BY 
                m."Last_name", m."First_name"
            ORDER BY 
                total_revenue DESC;
        """
        cur.execute(query, (start_date, end_date))
        result = cur.fetchall()
        logger.info(f"Получено {len(result)} записей для продаж по менеджерам")
        cur.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка при получении продаж по менеджерам: {str(e)}")
        return []

def get_arpu(start_date, end_date):
    logger.info(f"Запрос ARPU для {start_date} - {end_date}")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = """
            WITH 
            total_revenue AS (
                SELECT 
                    SUM(og."Sum_and_discont_og") AS revenue
                FROM 
                    "Order_goods" og
                JOIN 
                    "Order" o ON og."OrderID" = o."OrderID"
                WHERE 
                    o."Date_order" BETWEEN %s AND %s
            ),
            unique_customers AS (
                SELECT 
                    COUNT(DISTINCT o."CustomerID") AS customer_count
                FROM 
                    "Order" o
                WHERE 
                    o."Date_order" BETWEEN %s AND %s
            )
            SELECT 
                tr.revenue,
                uc.customer_count,
                (tr.revenue / NULLIF(uc.customer_count, 0)) AS arpu
            FROM 
                total_revenue tr, 
                unique_customers uc;
        """
        cur.execute(query, (start_date, end_date, start_date, end_date))
        result = cur.fetchone()
        logger.info(f"Получено ARPU: {result}")
        cur.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка при получении ARPU: {str(e)}")
        return (0, 0, 0)

def get_category_stats(start_date, end_date, category=None):
    logger.info(f"Запрос статистики по категориям для {start_date} - {end_date}, категория: {category}")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = """
            SELECT 
                cg."Category" AS category,
                SUM(COALESCE(og."Sum_and_discont_og", 0)) AS revenue,
                SUM(COALESCE(s."Price_supply", 0) * og."Quantity_goods") AS cost_price,
                SUM(COALESCE(og."Sum_and_discont_og", 0)) - SUM(COALESCE(s."Price_supply", 0) * og."Quantity_goods") AS gross_profit,
                COALESCE(ROUND(
                    ((SUM(COALESCE(og."Sum_and_discont_og", 0)) - SUM(COALESCE(s."Price_supply", 0) * og."Quantity_goods"))::numeric 
                    / NULLIF(SUM(COALESCE(og."Sum_and_discont_og", 0)), 0)) * 100, 2
                ), 0) AS margin_percent,
                COUNT(DISTINCT o."OrderID") AS order_count,
                SUM(og."Quantity_goods") AS items_sold
            FROM "Order" o
            JOIN "Order_goods" og ON o."OrderID" = og."OrderID"
            JOIN "Goods" g ON og."GoodID" = g."GoodID"
            JOIN "Category_goods" cg ON g."Category_goodsID" = cg."Category_goodsID"
            LEFT JOIN "Supply" s ON g."GoodID" = s."GoodsID" 
                AND s."Date_supply" = (SELECT MIN("Date_supply") FROM "Supply" WHERE "GoodsID" = g."GoodID")
            WHERE o."Date_order" BETWEEN %s AND %s
        """
        params = [start_date, end_date]
        if category:
            query += ' AND cg."Category" = %s'
            params.append(category)
        query += """
            GROUP BY cg."Category"
            ORDER BY margin_percent DESC;
        """
        cur.execute(query, params)
        result = cur.fetchall()
        logger.info(f"Получено {len(result)} записей для статистики по категориям")
        cur.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка при получении статистики по категориям: {str(e)}")
        return []

def get_profitability(start_date, end_date):
    logger.info(f"Запрос маржинальности (расширенный) для {start_date} - {end_date}")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = """
            WITH 
            -- Выручка
            revenue AS (
                SELECT COALESCE(SUM(og."Sum_and_discont_og"), 0) AS total_revenue
                FROM "Order" o
                JOIN "Order_goods" og ON o."OrderID" = og."OrderID"
                WHERE o."Date_order" BETWEEN %s AND %s
            ),
            -- Расходы на зарплаты
            salary_expenses AS (
                SELECT SUM("Salary") AS total_salary_expenses
                FROM "Staff"
            ),
            -- Расходы на аренду
            rent_expenses AS (
                SELECT COALESCE(SUM("Rental_price_month"), 0) AS total_rent_expenses
                FROM "Store"
            ),
            -- Себестоимость проданных товаров
            cost_of_goods_sold AS (
                SELECT COALESCE(SUM(s."Price_supply" * og."Quantity_goods"), 0) AS total_cogs
                FROM "Order" o
                JOIN "Order_goods" og ON o."OrderID" = og."OrderID"
                LEFT JOIN "Supply" s ON og."GoodID" = s."GoodsID"
                    AND s."Date_supply" = (
                        SELECT MIN(s2."Date_supply") 
                        FROM "Supply" s2 
                        WHERE s2."GoodsID" = s."GoodsID"
                        AND s2."Date_supply" <= o."Date_order"
                    )
                WHERE o."Date_order" BETWEEN %s AND %s
            ),
            -- Дополнительные расходы
            other_expenses AS (
                SELECT 
                    200000 AS marketing,
                    470000 AS logistics,
                    175000 AS utilities,
                    25000 AS mobile_phone,
                    60000 AS equipment_depreciation
            )
            SELECT 
                r.total_revenue,
                cogs.total_cogs,
                se.total_salary_expenses,
                re.total_rent_expenses,
                oe.marketing,
                oe.logistics,
                oe.utilities,
                oe.mobile_phone,
                oe.equipment_depreciation,
                r.total_revenue * 0.20 AS vat,
                (r.total_revenue - (r.total_revenue * 0.20) - cogs.total_cogs - se.total_salary_expenses - re.total_rent_expenses - 
                 oe.marketing - oe.logistics - oe.utilities - oe.mobile_phone - oe.equipment_depreciation) * 0.15 AS profit_tax,
                r.total_revenue - (r.total_revenue * 0.20) - cogs.total_cogs - se.total_salary_expenses - re.total_rent_expenses -
                oe.marketing - oe.logistics - oe.utilities - oe.mobile_phone - oe.equipment_depreciation -
                ((r.total_revenue - (r.total_revenue * 0.20) - cogs.total_cogs - se.total_salary_expenses - re.total_rent_expenses - 
                  oe.marketing - oe.logistics - oe.utilities - oe.mobile_phone - oe.equipment_depreciation) * 0.15) AS net_profit
            FROM 
                revenue r,
                salary_expenses se,
                rent_expenses re,
                cost_of_goods_sold cogs,
                other_expenses oe;
        """
        cur.execute(query, (start_date, end_date, start_date, end_date))
        result = cur.fetchone()
        logger.info(f"Получены данные маржинальности: {sanitize_log_data([result])}")
        cur.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка при получении расширенной маржинальности: {str(e)}")
        return None

# Добавляем фильтр для форматирования валюты
@app.template_filter('format_currency')
def format_currency(value):
    try:
        return "{:,.2f}".format(float(value))
    except (ValueError, TypeError):
        logger.error(f"Ошибка форматирования валюты для значения: {value}")
        return "0.00"

@app.route('/sales')
def sales_dashboard():
    period_type = request.args.get('period_type', 'custom')
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    selected_month = request.args.get('selected_month')
    selected_year = request.args.get('selected_year')
    selected_quarter = request.args.get('selected_quarter')
    selected_category = request.args.get('selected_category')

    # Установка диапазона дат по умолчанию (последние 30 дней)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)
    error_message = None

    try:
        if period_type == 'custom':
            if start_date_str and end_date_str:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
                if start_date > end_date:
                    raise ValueError("Начальная дата не может быть позже конечной")
                if end_date > datetime.now().date():
                    end_date = datetime.now().date()
                    logger.info("Конечная дата скорректирована до текущей даты")
                if (end_date - start_date).days > 365:
                    raise ValueError("Диапазон дат не должен превышать 1 год")
            else:
                logger.info("Даты для пользовательского периода не указаны, используются значения по умолчанию")
        elif period_type == 'month' and selected_month and selected_year:
            year = int(selected_year)
            month = int(selected_month)
            start_date = datetime(year, month, 1).date()
            end_date = start_date + relativedelta(months=1) - timedelta(days=1)
            if end_date > datetime.now().date():
                end_date = datetime.now().date()
                logger.info("Конечная дата месяца скорректирована до текущей даты")
        elif period_type == 'year' and selected_year:
            year = int(selected_year)
            start_date = datetime(year, 1, 1).date()
            end_date = datetime(year, 12, 31).date()
            if end_date > datetime.now().date():
                end_date = datetime.now().date()
                logger.info("Конечная дата года скорректирована до текущей даты")
        elif period_type == 'quarter' and selected_quarter and selected_year:
            year = int(selected_year)
            quarter = int(selected_quarter)
            month = (quarter - 1) * 3 + 1
            start_date = datetime(year, month, 1).date()
            end_date = start_date + relativedelta(months=3) - timedelta(days=1)
            if end_date > datetime.now().date():
                end_date = datetime.now().date()
                logger.info("Конечная дата квартала скорректирована до текущей даты")
        else:
            error_message = "Пожалуйста, выберите корректные параметры периода."
            logger.info(f"Некорректные параметры периода: period_type={period_type}, "
                       f"selected_month={selected_month}, selected_year={selected_year}, "
                       f"selected_quarter={selected_quarter}")

        # Проверка существования категории
        categories = get_categories()
        if selected_category and selected_category not in categories:
            error_message = f"Категория '{selected_category}' не существует."
            selected_category = None
            logger.warning(f"Выбрана несуществующая категория: {selected_category}")

        # Получение сводной статистики
        summary_stats = get_summary_stats(start_date, end_date)
        if not summary_stats['total_revenue']:
            logger.warning(f"Нет данных о продажах за период {start_date} - {end_date}")
            error_message = error_message or "Нет данных о продажах за выбранный период."

    except ValueError as e:
        logger.error(f"Ошибка при разборе дат: {str(e)}")
        error_message = f"Ошибка в датах: {str(e)}"
        categories = get_categories()
        summary_stats = get_summary_stats(start_date, end_date)

    # Остальной код остается без изменений
    logger.info(f"Получение данных для диапазона {start_date} - {end_date}")
    gross_profit = get_gross_profit(start_date, end_date)
    orders_count = get_orders_count(start_date, end_date)
    avg_order_value = get_avg_order_value(start_date, end_date)
    revenue_by_store = get_revenue_by_store(start_date, end_date, selected_category)
    orders_by_store = get_orders_by_store(start_date, end_date, selected_category)
    top_brands = get_top_brands(start_date, end_date)
    top_categories = get_top_categories(start_date, end_date)
    sales_by_manager = get_sales_by_manager(start_date, end_date)
    arpu_data = get_arpu(start_date, end_date)
    category_stats_data = get_category_stats(start_date, end_date, selected_category)
    profitability_data = get_profitability(start_date, end_date)

    # График валовой прибыли
    logger.info("Построение графика валовой прибыли")
    if gross_profit:
        df_gross = pd.DataFrame(gross_profit, columns=['date', 'revenue', 'purchase_cost', 'gross_profit'])
        gross_fig = go.Figure()
        gross_fig.add_trace(go.Scatter(
            x=df_gross['date'], y=df_gross['gross_profit'], 
            name='Валовая прибыль', line=dict(color='green', width=2),
            hovertemplate='Дата: %{x|%Y-%m-%d}<br>Валовая прибыль: ₽%{y:,.2f}<extra></extra>'
        ))
        gross_fig.add_trace(go.Scatter(
            x=df_gross['date'], y=df_gross['revenue'], 
            name='Выручка', line=dict(color='blue', width=2),
            hovertemplate='Дата: %{x|%Y-%m-%d}<br>Выручка: ₽%{y:,.2f}<extra></extra>'
        ))
        gross_fig.add_trace(go.Scatter(
            x=df_gross['date'], y=df_gross['purchase_cost'], 
            name='Закупочные расходы', line=dict(color='red', width=2, dash='dash'),
            hovertemplate='Дата: %{x|%Y-%m-%d}<br>Закупочные расходы: ₽%{y:,.2f}<extra></extra>'
        ))
        gross_fig.update_layout(
            title=dict(text='Валовая прибыль, выручка и расходы', font=dict(size=20)),
            xaxis_title='Дата',
            yaxis_title='Сумма, ₽',
            template='plotly_white',
            hovermode='x unified',
            showlegend=True,
            xaxis=dict(
                gridcolor='lightgrey',
                showgrid=True,
                tickformat='%Y-%m-%d',
                rangeslider=dict(visible=True),
                showline=True,
                linewidth=1,
                linecolor='black',
                mirror=True
            ),
            yaxis=dict(
                gridcolor='lightgrey',
                showgrid=True,
                tickformat=',.0f',
                showline=True,
                linewidth=1,
                linecolor='black',
                mirror=True
            ),
            font=dict(family='Arial', size=12),
            plot_bgcolor='white',
            margin=dict(l=50, r=50, t=80, b=50),
            legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0.8)')
        )
        gross_chart = gross_fig.to_html(full_html=False)
    else:
        logger.warning("Нет данных для графика валовой прибыли")
        gross_chart = "<p>Нет данных о валовой прибыли</p>"

    # График количества заказов
    logger.info("Построение графика количества заказов")
    if orders_count:
        df_orders = pd.DataFrame(orders_count, columns=['date', 'order_count'])
        orders_fig = go.Figure()
        orders_fig.add_trace(go.Scatter(
            x=df_orders['date'], y=df_orders['order_count'], 
            name='Заказы', line=dict(color='purple', width=2),
            hovertemplate='Дата: %{x|%Y-%m-%d}<br>Заказы: %{y}<extra></extra>'
        ))
        orders_fig.update_layout(
            title=dict(text='Количество заказов', font=dict(size=20)),
            xaxis_title='Дата',
            yaxis_title='Количество',
            template='plotly_white',
            hovermode='x unified',
            xaxis=dict(
                gridcolor='lightgrey',
                showgrid=True,
                tickformat='%Y-%m-%d',
                rangeslider=dict(visible=True),
                showline=True,
                linewidth=1,
                linecolor='black',
                mirror=True
            ),
            yaxis=dict(
                gridcolor='lightgrey',
                showgrid=True,
                tickformat=',.0f',
                showline=True,
                linewidth=1,
                linecolor='black',
                mirror=True
            ),
            font=dict(family='Arial', size=12),
            plot_bgcolor='white',
            margin=dict(l=50, r=50, t=80, b=50),
            legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0.8)')
        )
        orders_chart = orders_fig.to_html(full_html=False)
    else:
        logger.warning("Нет данных для графика количества заказов")
        orders_chart = "<p>Нет данных о заказах</p>"

    # График среднего чека
    logger.info("Построение графика среднего чека")
    if avg_order_value:
        df_avg = pd.DataFrame(avg_order_value, columns=['date', 'avg_order_value'])
        avg_fig = go.Figure()
        avg_fig.add_trace(go.Scatter(
            x=df_avg['date'], y=df_avg['avg_order_value'], 
            name='Средний чек', line=dict(color='orange', width=2),
            hovertemplate='Дата: %{x|%Y-%m-%d}<br>Средний чек: ₽%{y:,.2f}<extra></extra>'
        ))
        avg_fig.update_layout(
            title=dict(text='Средний чек', font=dict(size=20)),
            xaxis_title='Дата',
            yaxis_title='Сумма, ₽',
            template='plotly_white',
            hovermode='x unified',
            xaxis=dict(
                gridcolor='lightgrey',
                showgrid=True,
                tickformat='%Y-%m-%d',
                rangeslider=dict(visible=True),
                showline=True,
                linewidth=1,
                linecolor='black',
                mirror=True
            ),
            yaxis=dict(
                gridcolor='lightgrey',
                showgrid=True,
                tickformat=',.0f',
                showline=True,
                linewidth=1,
                linecolor='black',
                mirror=True
            ),
            font=dict(family='Arial', size=12),
            plot_bgcolor='white',
            margin=dict(l=50, r=50, t=80, b=50),
            legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0.8)')
        )
        avg_chart = avg_fig.to_html(full_html=False)
    else:
        logger.warning("Нет данных для графика среднего чека")
        avg_chart = "<p>Нет данных о среднем чеке</p>"

    # График выручки по магазинам
    logger.info("Построение графика выручки по магазинам")
    if revenue_by_store:
        df_revenue_store = pd.DataFrame(revenue_by_store, columns=['store_name', 'total_revenue'])
        title = 'Процент выручки по магазинам и сайту'
        if selected_category:
            title += f' (Категория: {selected_category})'
        revenue_store_fig = px.pie(
            df_revenue_store, names='store_name', values='total_revenue',
            title=title,
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        revenue_store_fig.update_traces(
            textinfo='percent+label',
            hovertemplate='Магазин: %{label}<br>Выручка: ₽%{value:,.2f}<br>Доля: %{percent}<extra></extra>'
        )
        revenue_store_fig.update_layout(
            template='plotly_white',
            font=dict(family='Arial', size=12),
            title_font=dict(size=20),
            showlegend=True,
            margin=dict(l=50, r=50, t=80, b=50),
            legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0.8)')
        )
        revenue_store_chart = revenue_store_fig.to_html(full_html=False)
    else:
        logger.warning("Нет данных для графика выручки по магазинам")
        revenue_store_chart = "<p>Нет данных о выручке по магазинам</p>"

    # График заказов по магазинам
    logger.info("Построение графика заказов по магазинам")
    if orders_by_store:
        df_orders_store = pd.DataFrame(orders_by_store, columns=['store_name', 'order_count'])
        title = 'Процент заказов по магазинам и сайту'
        if selected_category:
            title += f' (Категория: {selected_category})'
        orders_store_fig = px.pie(
            df_orders_store, names='store_name', values='order_count',
            title=title,
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        orders_store_fig.update_traces(
            textinfo='percent+label',
            hovertemplate='Магазин: %{label}<br>Заказы: %{value}<br>Доля: %{percent}<extra></extra>'
        )
        orders_store_fig.update_layout(
            template='plotly_white',
            font=dict(family='Arial', size=12),
            title_font=dict(size=20),
            showlegend=True,
            margin=dict(l=50, r=50, t=80, b=50),
            legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0.8)')
        )
        orders_store_chart = orders_store_fig.to_html(full_html=False)
    else:
        logger.warning("Нет данных для графика заказов по магазинам")
        orders_store_chart = "<p>Нет данных о заказах по магазинам</p>"

    # График топ брендов
    logger.info("Построение графика топ брендов")
    if top_brands:
        df_brands = pd.DataFrame(top_brands, columns=['brand', 'total_revenue'])
        brands_fig = px.bar(
            df_brands, x='total_revenue', y='brand', orientation='h',
            title='Топ-10 брендов по выручке',
            labels={'total_revenue': 'Выручка, ₽', 'brand': 'Бренд'},
            color='total_revenue',
            color_continuous_scale='Blues'
        )
        brands_fig.update_traces(
            hovertemplate='Бренд: %{y}<br>Выручка: ₽%{x:,.2f}<extra></extra>'
        )
        brands_fig.update_layout(
            template='plotly_white',
            font=dict(family='Arial', size=12),
            title_font=dict(size=20),
            xaxis=dict(
                gridcolor='lightgrey',
                showgrid=True,
                tickformat=',.0f',
                showline=True,
                linewidth=1,
                linecolor='black',
                mirror=True
            ),
            yaxis=dict(
                showline=True,
                linewidth=1,
                linecolor='black',
                mirror=True
            ),
            plot_bgcolor='white',
            margin=dict(l=50, r=50, t=80, b=50),
            showlegend=False
        )
        brands_chart = brands_fig.to_html(full_html=False)
    else:
        logger.warning("Нет данных для графика топ брендов")
        brands_chart = "<p>Нет данных о брендах</p>"

    # График топ категорий
    logger.info("Построение графика топ категорий")
    if top_categories:
        df_categories = pd.DataFrame(top_categories, columns=['category', 'total_revenue'])
        categories_fig = px.bar(
            df_categories, x='total_revenue', y='category', orientation='h',
            title='Топ-10 категорий по выручке',
            labels={'total_revenue': 'Выручка, ₽', 'category': 'Категория'},
            color='total_revenue',
            color_continuous_scale='Greens'
        )
        categories_fig.update_traces(
            hovertemplate='Категория: %{y}<br>Выручка: ₽%{x:,.2f}<extra></extra>'
        )
        categories_fig.update_layout(
            template='plotly_white',
            font=dict(family='Arial', size=12),
            title_font=dict(size=20),
            xaxis=dict(
                gridcolor='lightgrey',
                showgrid=True,
                tickformat=',.0f',
                showline=True,
                linewidth=1,
                linecolor='black',
                mirror=True
            ),
            yaxis=dict(
                showline=True,
                linewidth=1,
                linecolor='black',
                mirror=True
            ),
            plot_bgcolor='white',
            margin=dict(l=50, r=50, t=80, b=50),
            showlegend=False
        )
        categories_chart = categories_fig.to_html(full_html=False)
    else:
        logger.warning("Нет данных для графика топ категорий")
        categories_chart = "<p>Нет данных о категориях</p>"

    # График продаж по менеджерам
    logger.info("Построение графика продаж по менеджерам")
    if sales_by_manager:
        df_managers = pd.DataFrame(sales_by_manager, columns=['manager_name', 'total_revenue'])
        managers_fig = px.bar(
            df_managers, x='total_revenue', y='manager_name', orientation='h',
            title='Продажи по менеджерам',
            labels={'total_revenue': 'Выручка, ₽', 'manager_name': 'Менеджер'},
            color='total_revenue',
            color_continuous_scale='Reds'
        )
        managers_fig.update_traces(
            hovertemplate='Менеджер: %{y}<br>Выручка: ₽%{x:,.2f}<extra></extra>'
        )
        managers_fig.update_layout(
            template='plotly_white',
            font=dict(family='Arial', size=12),
            title_font=dict(size=20),
            xaxis=dict(
                gridcolor='lightgrey',
                showgrid=True,
                tickformat=',.0f',
                showline=True,
                linewidth=1,
                linecolor='black',
                mirror=True
            ),
            yaxis=dict(
                showline=True,
                linewidth=1,
                linecolor='black',
                mirror=True
            ),
            plot_bgcolor='white',
            margin=dict(l=50, r=50, t=80, b=50),
            showlegend=False
        )
        managers_chart = managers_fig.to_html(full_html=False)
    else:
        logger.warning("Нет данных для графика продаж по менеджерам")
        managers_chart = "<p>Нет данных о продажах по менеджерам</p>"

    # График ARPU
    logger.info("Построение графика ARPU")
    if arpu_data and arpu_data[2]:
        arpu_fig = go.Figure()
        arpu_fig.add_trace(go.Indicator(
            mode="gauge+number",
            value=float(arpu_data[2]),
            title={'text': "ARPU (Средняя выручка на пользователя)", 'font': {'size': 20}},
            gauge={
                'axis': {'range': [0, max(float(arpu_data[2]) * 1.5, 1000)], 'tickformat': ',.0f'},
                'bar': {'color': 'blue'},
                'steps': [
                    {'range': [0, float(arpu_data[2]) * 0.5], 'color': 'lightgray'},
                    {'range': [float(arpu_data[2]) * 0.5, float(arpu_data[2])], 'color': 'gray'}
                ],
                'threshold': {
                    'line': {'color': 'red', 'width': 4},
                    'thickness': 0.75,
                    'value': float(arpu_data[2])
                }
            }
        ))
        arpu_fig.update_layout(
            template='plotly_white',
            font=dict(family='Arial', size=12),
            margin=dict(l=50, r=50, t=80, b=50),
            height=400
        )
        arpu_chart = arpu_fig.to_html(full_html=False)
    else:
        logger.warning("Нет данных для графика ARPU")
        arpu_chart = "<p>Нет данных для ARPU</p>"

    # График финансовых показателей по категориям
    logger.info("Построение графика финансовых показателей по категориям")
    if category_stats_data:
        df_category_stats = pd.DataFrame(category_stats_data, columns=[
            'category', 'revenue', 'cost_price', 'gross_profit', 'margin_percent', 'order_count', 'items_sold'
        ])
        if selected_category:
            # Для одной категории показываем индикатор маржинальности и таблицу с метриками
            margin_fig = go.Figure()
            margin_fig.add_trace(go.Indicator(
                mode="gauge+number",
                value=float(df_category_stats['margin_percent'].iloc[0]),
                title={'text': f"Маржинальность (%) для {selected_category}", 'font': {'size': 20}},
                gauge={
                    'axis': {'range': [0, 100], 'tickformat': ',.0f'},
                    'bar': {'color': 'green'},
                    'steps': [
                        {'range': [0, 40], 'color': 'lightgray'},
                        {'range': [40, 60], 'color': 'gray'},
                        {'range': [60, 100], 'color': 'darkgray'}
                    ],
                    'threshold': {
                        'line': {'color': 'red', 'width': 4},
                        'thickness': 0.75,
                        'value': float(df_category_stats['margin_percent'].iloc[0])
                    }
                },
                domain={'row': 0, 'column': 0}
            ))
            margin_fig.add_trace(go.Table(
                header=dict(
                    values=['Метрика', 'Значение'],
                    fill_color='lightblue',
                    align='center',
                    font=dict(size=14, color='black'),
                    height=40
                ),
                cells=dict(
                    values=[
                        ['Выручка, ₽', 'Себестоимость, ₽', 'Валовая прибыль, ₽', 'Маржинальность, %', 'Заказы', 'Товаров продано'],
                        [
                            f"{df_category_stats['revenue'].iloc[0]:,.2f}",
                            f"{df_category_stats['cost_price'].iloc[0]:,.2f}",
                            f"{df_category_stats['gross_profit'].iloc[0]:,.2f}",
                            f"{df_category_stats['margin_percent'].iloc[0]:,.2f}",
                            f"{df_category_stats['order_count'].iloc[0]}",
                            f"{df_category_stats['items_sold'].iloc[0]}"
                        ]
                    ],
                    fill_color='white',
                    align='center',
                    font=dict(size=12, color='black'),
                    height=30
                ),
                domain={'row': 1, 'column': 0}
            ))
            margin_fig.update_layout(
                template='plotly_white',
                font=dict(family='Arial', size=12),
                margin=dict(l=50, r=50, t=80, b=50),
                height=600,
                grid={'rows': 2, 'columns': 1},
                showlegend=False
            )
        else:
            margin_fig = go.Figure()
            margin_fig.add_trace(go.Bar(
                x=df_category_stats['category'],
                y=df_category_stats['revenue'],
                name='Выручка',
                marker_color='blue',
                hovertemplate='Категория: %{x}<br>Выручка: ₽%{y:,.2f}<extra></extra>'
            ))
            margin_fig.add_trace(go.Bar(
                x=df_category_stats['category'],
                y=df_category_stats['cost_price'],
                name='Себестоимость',
                marker_color='red',
                hovertemplate='Категория: %{x}<br>Себестоимость: ₽%{y:,.2f}<extra></extra>'
            ))
            margin_fig.add_trace(go.Bar(
                x=df_category_stats['category'],
                y=df_category_stats['gross_profit'],
                name='Валовая прибыль',
                marker_color='green',
                hovertemplate='Категория: %{x}<br>Валовая прибыль: ₽%{y:,.2f}<extra></extra>'
            ))
            margin_fig.update_layout(
                title=dict(text='Финансовые показатели по категориям товаров', font=dict(size=20)),
                xaxis_title='Категория',
                yaxis_title='Сумма, ₽',
                template='plotly_white',
                barmode='group',
                hovermode='x unified',
                showlegend=True,
                xaxis=dict(
                    tickangle=45,
                    showline=True,
                    linewidth=1,
                    linecolor='black',
                    mirror=True
                ),
                yaxis=dict(
                    gridcolor='lightgrey',
                    showgrid=True,
                    tickformat=',.0f',
                    showline=True,
                    linewidth=1,
                    linecolor='black',
                    mirror=True
                ),
                font=dict(family='Arial', size=12),
                plot_bgcolor='white',
                margin=dict(l=50, r=50, t=80, b=100),
                height=500,
                legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0.8)')
            )
        margin_chart = margin_fig.to_html(full_html=False)
    else:
        logger.warning("Нет данных для графика финансовых показателей по категориям")
        margin_chart = "<p>Нет данных для финансовых показателей по категориям</p>"

    # График расширенных финансовых показателей
    logger.info("Построение графика расширенных финансовых показателей")
    if profitability_data:
        labels = [
            'Выручка', 'Себестоимость', 'Зарплаты', 'Аренда', 'Маркетинг', 'Логистика',
            'Коммунальные', 'Связь', 'Амортизация', 'НДС', 'Налог на прибыль', 'Чистая прибыль'
        ]
        values = [
            profitability_data[0],  # total_revenue
            profitability_data[1],  # total_cogs
            profitability_data[2],  # total_salary_expenses
            profitability_data[3],  # total_rent_expenses
            profitability_data[4],  # marketing
            profitability_data[5],  # logistics
            profitability_data[6],  # utilities
            profitability_data[7],  # mobile_phone
            profitability_data[8],  # equipment_depreciation
            profitability_data[9],  # vat
            profitability_data[10], # profit_tax
            profitability_data[11]  # net_profit
        ]
        colors = [
            'blue', 'red', 'orange', 'purple', 'pink', 'brown', 'gray', 'cyan', 'magenta',
            'darkred', 'darkgreen', 'green'
        ]
        profitability_fig = go.Figure()
        profitability_fig.add_trace(go.Bar(
            x=labels,
            y=values,
            marker_color=colors,
            hovertemplate='%{x}: ₽%{y:,.2f}<extra></extra>'
        ))
        profitability_fig.update_layout(
            title=dict(text='Расширенные финансовые показатели', font=dict(size=20)),
            xaxis_title='Категория',
            yaxis_title='Сумма, ₽',
            template='plotly_white',
            showlegend=False,
            xaxis=dict(
                tickangle=45,
                showline=True,
                linewidth=1,
                linecolor='black',
                mirror=True
            ),
            yaxis=dict(
                gridcolor='lightgrey',
                showgrid=True,
                tickformat=',.0f',
                showline=True,
                linewidth=1,
                linecolor='black',
                mirror=True
            ),
            font=dict(family='Arial', size=12),
            plot_bgcolor='white',
            margin=dict(l=50, r=50, t=80, b=100),
            height=500
        )
        profitability_chart = profitability_fig.to_html(full_html=False)
    else:
        logger.warning("Нет данных для графика расширенной маржинальности")
        profitability_chart = "<p>Нет данных для расширенных финансовых показателей</p>"

    return render_template(
        'sales_dashboard.html',
        gross_chart=gross_chart,
        orders_chart=orders_chart,
        avg_chart=avg_chart,
        revenue_store_chart=revenue_store_chart,
        orders_store_chart=orders_store_chart,
        brands_chart=brands_chart,
        categories_chart=categories_chart,
        managers_chart=managers_chart,
        arpu_chart=arpu_chart,
        margin_chart=margin_chart,
        profitability_chart=profitability_chart,
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d'),
        period_type=period_type,
        selected_month=selected_month or '',
        selected_year=selected_year or '',
        selected_quarter=selected_quarter or '',
        categories=categories,
        selected_category=selected_category or '',
        summary_stats=summary_stats,
        error_message=error_message
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)