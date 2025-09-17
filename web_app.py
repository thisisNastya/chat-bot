from flask import Flask, render_template, request, send_from_directory, jsonify
import os
import psycopg2
from datetime import datetime, timedelta
from calendar import monthrange, month_name
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import logging
from dotenv import load_dotenv
from cryptography.fernet import Fernet

app = Flask(__name__)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

#Загружаем переменные окружения из .env
load_dotenv()

ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
cipher = Fernet(ENCRYPTION_KEY)

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "sslmode": "require"
}

# Функция маскирует чувствительные данные 
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

def get_countries():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT \"Country_name\" FROM public.\"Country\" ORDER BY \"Country_name\";")
        countries = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        return countries
    except Exception as e:
        logger.error(f"Ошибка при получении списка стран: {str(e)}")
        return []

def get_categories():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT \"Category\" FROM public.\"Category_goods\" ORDER BY \"Category\";")
        categories = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        return categories
    except Exception as e:
        logger.error(f"Ошибка при получении списка категорий: {str(e)}")
        return []

def get_goods_list(filter_type=None, search_name=None, search_id=None, search_country=None, search_category=None):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = """
            SELECT "GoodID", "Goods"
            FROM public."Goods" g
            JOIN public."Category_goods" cg ON g."Category_goodsID" = cg."Category_goodsID"
            JOIN public."Country" co ON g."CountryID" = co."CountryID"
            WHERE 1=1
        """
        params = []
        if filter_type == 'name' and search_name:
            # Удаляем специальные символы из поискового запроса
            search_name_clean = ''.join(c for c in search_name if c.isalnum() or c.isspace())
            search_name_clean = search_name_clean.replace(' ', '%')
            query += " AND REGEXP_REPLACE(\"Goods\", '[^а-яА-Яa-zA-Z0-9 ]', '', 'g') ILIKE %s"
            params.append(f'%{search_name_clean}%')
        elif filter_type == 'id' and search_id:
            query += " AND \"GoodID\" = %s"
            params.append(search_id)
        elif filter_type == 'category_country' and (search_category or search_country):
            if search_category:
                query += " AND cg.\"Category\" = %s"
                params.append(search_category)
            if search_country:
                query += " AND co.\"Country_name\" = %s"
                params.append(search_country)
        elif filter_type == 'name_country' and (search_name or search_country):
            if search_name:
                search_name_clean = ''.join(c for c in search_name if c.isalnum() or c.isspace())
                search_name_clean = search_name_clean.replace(' ', '%')
                query += " AND REGEXP_REPLACE(\"Goods\", '[^а-яА-Яa-zA-Z0-9 ]', '', 'g') ILIKE %s"
                params.append(f'%{search_name_clean}%')
            if search_country:
                query += " AND co.\"Country_name\" = %s"
                params.append(search_country)
        query += " ORDER BY \"Goods\" ASC;"
        cur.execute(query, params)
        goods = cur.fetchall()
        cur.close()
        conn.close()
        return goods
    except Exception as e:
        logger.error(f"Ошибка при получении списка товаров: {str(e)}")
        return []

def get_product_info(good_id):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                g."GoodID", g."Goods", g."Brend", g."Type_good", 
                cg."Category", co."Country_name", 
                pg."Goods_price", d."Discount_amount", g."Storage_life",
                (pg."Goods_price" * (1 - d."Discount_amount"/100.0))::numeric(10,2) AS "Discounted_price"
            FROM public."Goods" g
            JOIN public."Category_goods" cg ON g."Category_goodsID" = cg."Category_goodsID"
            JOIN public."Country" co ON g."CountryID" = co."CountryID"
            JOIN public."Price_goods" pg ON g."PriceID" = pg."PriceID"
            JOIN public."Discount" d ON g."DiscountID" = d."DiscountID"
            WHERE g."GoodID" = %s;
        """, (good_id,))
        info = cur.fetchone()
        cur.close()
        conn.close()
        return info
    except Exception as e:
        logger.error(f"Ошибка при получении информации о товаре: {str(e)}")
        return None

def get_product_popularity(good_id):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        start_date = datetime.now().date() - timedelta(days=60)
        cur.execute("""
            SELECT 
                DATE_TRUNC('week', o."Date_order")::date AS "week_start",
                COUNT(og."Order_goodsID") AS "order_count",
                SUM(og."Quantity_goods") AS "total_quantity"
            FROM public."Goods" g
            JOIN public."Order_goods" og ON g."GoodID" = og."GoodID"
            JOIN public."Order" o ON og."OrderID" = o."OrderID"
            WHERE g."GoodID" = %s
              AND o."Date_order" >= %s
            GROUP BY "week_start"
            ORDER BY "week_start";
        """, (good_id, start_date))
        result = cur.fetchall()
        cur.close()
        conn.close()
        return result if result else []
    except Exception as e:
        logger.error(f"Ошибка при получении данных о популярности по неделям: {str(e)}")
        return []

def get_product_availability(good_id):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                s."City", 
                s."Street", 
                s."Building", 
                s."Name" AS "Name_stock",
                ss."Goods_quantity" AS "Quantity"
            FROM public."Goods" g
            LEFT JOIN public."Store_stock" ss ON g."GoodID" = ss."GoodID"
            LEFT JOIN public."Store" s ON ss."StoreID" = s."StoreID"
            WHERE g."GoodID" = %s
            ORDER BY ss."Goods_quantity" DESC;
        """, (good_id,))
        availability = cur.fetchall()
        cur.close()
        conn.close()
        return availability
    except Exception as e:
        logger.error(f"Ошибка при получении данных о наличии: {str(e)}")
        return []

def get_product_suppliers(good_id):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                s."Name_suppliers" AS "Поставщик",
                s."Contact_person" AS "Контактное лицо",
                s."Number_phone_suppliers" AS "Телефон поставщика",
                sp."Date_supply" AS "Дата последней поставки",
                sp."Price_supply" AS "Цена поставки"
            FROM 
                public."Goods" g
            LEFT JOIN 
                public."Supply" sp ON g."GoodID" = sp."GoodsID"
            LEFT JOIN 
                public."Suppliers" s ON sp."SuppliersID" = s."SuppliersID"
            WHERE 
                g."GoodID" = %s;
        """, (good_id,))
        suppliers = cur.fetchall()
        cur.close()
        conn.close()
        return suppliers
    except Exception as e:
        logger.error(f"Ошибка при получении данных о поставщиках: {str(e)}")
        return []

def get_product_ratings(good_id):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                ROUND(AVG(r."Rating")::numeric, 2) AS avg_rating,
                COUNT(r."Rating") AS rating_count
            FROM public."Goods" g
            LEFT JOIN public."Rating_goods" r ON g."GoodID" = r."GoodID"
            WHERE g."GoodID" = %s
            GROUP BY g."GoodID";
        """, (good_id,))
        ratings = cur.fetchone()
        ratings = {'avg_rating': ratings[0], 'rating_count': ratings[1]} if ratings else {'avg_rating': None, 'rating_count': 0}
        
        cur.execute("""
            SELECT r."Rating", COUNT(r."Rating") AS rating_count
            FROM public."Rating_goods" r
            WHERE r."GoodID" = %s
            GROUP BY r."Rating"
            ORDER BY r."Rating";
        """, (good_id,))
        rating_distribution = cur.fetchall()
        
        cur.close()
        conn.close()
        logger.info(f"Рейтинги для GoodID {good_id}: {ratings}")
        logger.info(f"Распределение оценок для GoodID {good_id}: {rating_distribution}")
        return ratings, rating_distribution
    except Exception as e:
        logger.error(f"Ошибка при получении оценок: {str(e)}")
        return {'avg_rating': None, 'rating_count': 0}, []

def get_sales_dynamics(good_id, start_date=None, end_date=None):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = """
            SELECT 
                DATE_TRUNC('day', o."Date_order")::date AS "date",
                COALESCE(SUM(og."Quantity_goods"), 0) AS "quantity",
                COALESCE(SUM(og."Sum_and_discont_og"), 0) AS "revenue"
            FROM public."Goods" g
            LEFT JOIN public."Order_goods" og ON g."GoodID" = og."GoodID"
            LEFT JOIN public."Order" o ON og."OrderID" = o."OrderID"
            WHERE g."GoodID" = %s
        """
        params = [good_id]
        if start_date and end_date:
            query += " AND o.\"Date_order\" BETWEEN %s AND %s"
            params.extend([start_date, end_date])
        elif start_date:
            query += " AND o.\"Date_order\" >= %s"
            params.append(start_date)
        elif end_date:
            query += " AND o.\"Date_order\" <= %s"
            params.append(end_date)
        query += " GROUP BY \"date\" ORDER BY \"date\";"
        cur.execute(query, params)
        sales = cur.fetchall()
        cur.close()
        conn.close()
        return sales
    except Exception as e:
        logger.error(f"Ошибка при получении динамики продаж: {str(e)}")
        return []

def get_gender_distribution(good_id):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                c."Gender",
                COUNT(*) AS "purchases"
            FROM public."Goods" g
            JOIN public."Order_goods" og ON g."GoodID" = og."GoodID"
            JOIN public."Order" o ON og."OrderID" = o."OrderID"
            JOIN public."Customer" c ON o."CustomerID" = c."CustomerID"
            WHERE g."GoodID" = %s
            GROUP BY c."Gender"
            ORDER BY "purchases" DESC;
        """, (good_id,))
        gender_data = cur.fetchall()
        cur.close()
        conn.close()
        return gender_data
    except Exception as e:
        logger.error(f"Ошибка при получении данных о поле покупателей: {str(e)}")
        return []

def get_holiday_seasonality(good_id):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                CASE 
                    WHEN (EXTRACT(MONTH FROM o."Date_order") = 12 AND EXTRACT(DAY FROM o."Date_order") = 31) 
                    OR (EXTRACT(MONTH FROM o."Date_order") = 1 AND EXTRACT(DAY FROM o."Date_order") = 1) 
                    THEN 'Новый Год'
                    WHEN (EXTRACT(MONTH FROM o."Date_order") = 12 AND EXTRACT(DAY FROM o."Date_order") BETWEEN 25 AND 30) 
                    THEN 'Предновогодняя неделя'
                    WHEN (EXTRACT(MONTH FROM o."Date_order") = 2 AND EXTRACT(DAY FROM o."Date_order") = 14) 
                    THEN 'День Влюбленных'
                    WHEN (EXTRACT(MONTH FROM o."Date_order") = 2 AND EXTRACT(DAY FROM o."Date_order") = 23) 
                    THEN '23 Февраля'
                    WHEN (EXTRACT(MONTH FROM o."Date_order") = 3 AND EXTRACT(DAY FROM o."Date_order") = 8) 
                    THEN '8 Марта'
                    WHEN (EXTRACT(MONTH FROM o."Date_order") = 3 AND EXTRACT(DAY FROM o."Date_order") BETWEEN 1 AND 7) 
                    THEN 'Перед 8 Марта'
                    WHEN (EXTRACT(MONTH FROM o."Date_order") = 2 AND EXTRACT(DAY FROM o."Date_order") BETWEEN 7 AND 13) 
                    THEN 'Перед 14 Февраля'
                    WHEN (EXTRACT(MONTH FROM o."Date_order") = EXTRACT(MONTH FROM c."Date_birthday") 
                         AND EXTRACT(DAY FROM o."Date_order") = EXTRACT(DAY FROM c."Date_birthday")) 
                    THEN 'День Рождения'
                    ELSE 'Обычный день'
                END AS "holiday",
                COUNT(*) AS "total_purchases"
            FROM public."Goods" g
            JOIN public."Order_goods" og ON g."GoodID" = og."GoodID"
            JOIN public."Order" o ON og."OrderID" = o."OrderID"
            JOIN public."Customer" c ON o."CustomerID" = c."CustomerID"
            WHERE g."GoodID" = %s
            GROUP BY "holiday"
            ORDER BY "total_purchases" DESC;
        """, (good_id,))
        holiday_data = cur.fetchall()
        cur.close()
        conn.close()
        logger.info(f"Данные о сезонности для GoodID {good_id}: {holiday_data}")
        return holiday_data
    except Exception as e:
        logger.error(f"Ошибка при получении данных о праздниках и сезонности: {str(e)}")
        return []

@app.route('/static/images_BD/<int:good_id>')
def serve_image(good_id):
    logger.info(f"Запрос изображения для GoodID: {good_id}")
    image_dir = os.path.join(app.static_folder, 'images_BD')
    for ext in ['jpg', 'jpeg']:
        image_path = f"{good_id}.{ext}"
        if os.path.exists(os.path.join(image_dir, image_path)):
            logger.info(f"Найдено изображение: {image_path}")
            return send_from_directory(image_dir, image_path)
    logger.warning(f"Изображение для GoodID {good_id} не найдено")
    placeholder = 'placeholder.jpg'
    placeholder_path = os.path.join(image_dir, placeholder)
    if os.path.exists(placeholder_path):
        return send_from_directory(image_dir, placeholder)
    logger.error(f"Заглушка {placeholder} не найдена")

@app.route('/search_goods')
def search_goods():
    search_name = request.args.get('search_name', default='')
    search_country = request.args.get('search_country', default='')
    filter_type = 'name' if not search_country else 'name_country'
    goods = get_goods_list(filter_type=filter_type, search_name=search_name, search_country=search_country)
    return jsonify([{'id': good[0], 'name': good[1]} for good in goods])

@app.route('/products')
def product_analysis():
    good_id = request.args.get('good_id', type=int, default=None)
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    filter_type = request.args.get('filter_type')
    search_name = request.args.get('search_name')
    search_id = request.args.get('search_id', type=int)
    search_country = request.args.get('search_country')
    search_category = request.args.get('search_category')
    show_filter = request.args.get('show_filter', default='false') == 'true'
    
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date() if end_date_str else None
    if not start_date and not end_date:
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=180)
    
    countries = get_countries()
    categories = get_categories()
    goods = get_goods_list(filter_type, search_name, search_id, search_country, search_category)
    
    if not good_id and goods and not show_filter:
        good_id = goods[0][0]
    
    product_info = get_product_info(good_id) if good_id else None
    popularity_data = get_product_popularity(good_id) if good_id else []
    sales_dynamics = get_sales_dynamics(good_id, start_date, end_date) if good_id else []
    gender_distribution = get_gender_distribution(good_id) if good_id else []
    holiday_seasonality = get_holiday_seasonality(good_id) if good_id else []
    availability = get_product_availability(good_id) if good_id else []
    suppliers = get_product_suppliers(good_id) if good_id else []
    ratings, rating_distribution = get_product_ratings(good_id) if good_id else ({'avg_rating': None, 'rating_count': 0}, [])
    
    if product_info:
        info_card = {
            'id': product_info[0], 
            'name': product_info[1],
            'brand': product_info[2],
            'type': product_info[3],
            'category': product_info[4],
            'country': product_info[5],
            'price': product_info[6],
            'discount': product_info[7],
            'storage_life': product_info[8],
            'discounted_price': product_info[9]
        }
    else:
        info_card = None
    
    if popularity_data:
        df_popularity = pd.DataFrame(popularity_data, columns=['week_start', 'order_count', 'total_quantity'])
        pop_fig = go.Figure()
        pop_fig.add_trace(go.Bar(
            x=df_popularity['week_start'],
            y=df_popularity['order_count'],
            name='Кол-во заказов',
            marker_color='blue'
        ))
        pop_fig.add_trace(go.Bar(
            x=df_popularity['week_start'],
            y=df_popularity['total_quantity'],
            name='Кол-во товаров',
            marker_color='orange'
        ))
        pop_fig.update_layout(
            title='Популярность товара (последние 2 месяца)',
            xaxis_title='Неделя',
            yaxis_title='Кол-во',
            barmode='group',
            template='plotly_white'
        )
        pop_chart = pop_fig.to_html(full_html=False)
    else:
        pop_chart = "<p>Нет данных о популярности</p>"
    
    show_date_filter = bool(sales_dynamics)
    if sales_dynamics:
        df = pd.DataFrame(sales_dynamics, columns=['date', 'quantity', 'revenue'])
        sales_fig = go.Figure()
        sales_fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['quantity'],
            name='Кол-во продаж',
            line=dict(color='blue'),
            yaxis='y1'
        ))
        sales_fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['revenue'],
            name='Выручка',
            line=dict(color='green'),
            yaxis='y2'
        ))
        sales_fig.update_layout(
            xaxis=dict(title='Дата', rangeslider=dict(visible=True), type='date'),
            yaxis=dict(title='Кол-во продаж', titlefont=dict(color='blue'), tickfont=dict(color='blue')),
            yaxis2=dict(title='Выручка, ₽', titlefont=dict(color='green'), tickfont=dict(color='green'), overlaying='y', side='right'),
            template='plotly_white',
            legend=dict(x=0.01, y=0.99),
            hovermode='x unified'
        )
        sales_chart = sales_fig.to_html(full_html=False)
    else:
        sales_chart = "<p>Нет данных о продажах</p>"
    
    if gender_distribution:
        df_gender = pd.DataFrame(gender_distribution, columns=['gender', 'purchases'])
        gender_fig = px.pie(df_gender, names='gender', values='purchases',
                           title='Распределение покупок по полу',
                           template='plotly_white')
        gender_chart = gender_fig.to_html(full_html=False)
    else:
        gender_chart = "<p>Нет данных о поле покупателей</p>"
    
    if holiday_seasonality:
        df_holiday = pd.DataFrame(holiday_seasonality, columns=['holiday', 'purchases'])
        holiday_fig = px.bar(df_holiday, x='holiday', y='purchases',
                            title='Покупки по дням',
                            labels={'holiday': 'Дни', 'purchases': 'Кол-во покупок'},
                            template='plotly_white')
        holiday_chart = holiday_fig.to_html(full_html=False)
    else:
        holiday_chart = "<p>Нет данных о сезонности</p>"
    
    if rating_distribution:
        df_ratings = pd.DataFrame(rating_distribution, columns=['rating', 'count'])
        rating_fig = px.bar(df_ratings, y='rating', x='count',
                           title='Распределение оценок товара',
                           labels={'rating': 'Оценка', 'count': 'Кол-во'},
                           orientation='h',
                           template='plotly_white')
        rating_fig.update_layout(yaxis=dict(tickmode='array', tickvals=[1, 2, 3, 4, 5]))
        rating_chart = rating_fig.to_html(full_html=False)
    else:
        rating_chart = "<p>Нет данных о рейтингах</p>"
    
    return render_template(
        'analysis.html',
        goods=goods,
        selected_good_id=good_id,
        info_card=info_card,
        pop_chart=pop_chart,
        sales_chart=sales_chart,
        gender_chart=gender_chart,
        holiday_chart=holiday_chart,
        rating_chart=rating_chart,
        month_names=[month_name[i] for i in range(1, 13)],
        start_date=start_date.strftime('%Y-%m-%d') if start_date else '',
        end_date=end_date.strftime('%Y-%m-%d') if end_date else '',
        show_date_filter=show_date_filter,
        availability=availability,
        suppliers=suppliers,
        ratings=ratings,
        show_filter=show_filter,
        countries=countries,
        categories=categories,
        search_name=search_name,
        search_id=search_id,
        search_country=search_country,
        search_category=search_category,
        filter_type=filter_type
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)