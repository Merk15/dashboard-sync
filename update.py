from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv
import os
import json

# === 🌍 Загрузка переменных окружения ===
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT"))  # преобразуем в int
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME")

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# === 🔕 Дата до 00:00 текущего дня ===
now = datetime.now()
end_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
formatted_date = end_date.strftime('%Y-%m-%d %H:%M:%S')

# === 🔕 SQL с JOIN и CASE ===
query = f"""
SELECT
    p.payment_id,
    p.parent_payment_id,
    p.supporter_id,
    p.supporter_account_id,
    ps.name AS payment_service_id,
    pm.name AS payment_method_id,
    pt.name AS payment_type_id,
    a.name AS appeal_id,
    p.payment_date,
    p.payment_data,
    p.transaction_id,
    p.parent_transaction_id,
    p.subscription_id,
    CASE
        WHEN p.regular = 0 THEN 'разовое'
        WHEN p.regular = 1 THEN 'первое'
        WHEN p.regular = 2 THEN 'повторное'
        ELSE 'неизвестно'
    END AS regular,
    p.amount,
    p.net_amount,
    p.fee,
    p.is_success,
    p.bankcard_expire,
    p.bankcard_service,
    p.bank,
    p.referer,
    p.first_referer,
    p.utm_source,
    p.utm_medium,
    p.utm_term,
    p.utm_content,
    p.utm_campaign,
    p.ip,
    p.subscription_amount,
    p.subscription_date,
    p.subscription_status,
    p.last_rebill_date,
    p.next_rebill_date,
    p.subscription_status_date,
    p.try_rebill_count
FROM public.payment p
LEFT JOIN public.payment_service ps ON p.payment_service_id = ps.payment_service_id
LEFT JOIN public.payment_method pm ON p.payment_method_id = pm.payment_method_id
LEFT JOIN public.payment_type pt ON p.payment_type_id = pt.payment_type_id
LEFT JOIN public.appeal a ON p.appeal_id = a.appeal_id
WHERE p.is_success = TRUE
  AND p.create_date < '{formatted_date}'
"""

# === 🔀 Загрузка из PostgreSQL ===
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
df = pd.read_sql_query(query, engine)

# === 🔕 Разделение payment_date ===
df["payment_date"] = pd.to_datetime(df["payment_date"])
df = df.sort_values(by="payment_date", ascending=False)
payment_time = df["payment_date"].dt.strftime('%H:%M:%S')
df.insert(df.columns.get_loc("payment_data"), "payment_time", payment_time)
df["payment_date"] = df["payment_date"].dt.strftime('%d.%m.%Y')

# === 📄 Убираем NaT и форматируем даты ===
date_cols = ["subscription_date", "last_rebill_date", "next_rebill_date", "subscription_status_date", "bankcard_expire"]
for col in date_cols:
    df[col] = pd.to_datetime(df[col], errors='coerce')
    df[col] = df[col].dt.strftime('%d.%m.%Y')
    df[col] = df[col].fillna("")

# === 📊 Преобразуем числовые столбцы и делаем str с запятой ===
numeric_cols = ["amount", "net_amount", "fee", "subscription_amount"]
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(2).apply(lambda x: str(x).replace('.', ','))

# === 🔘 Очистка пустых текстовых значений от пробелов, замена на пустую строку ===
text_cols = ["utm_medium"]
for col in text_cols:
    df[col] = df[col].apply(lambda x: "" if pd.isna(x) or str(x).strip() in ["", "nan", "None"] else str(x).strip())

# === 🔢 Строковые значения ===
for col in df.columns:
    if col not in ["payment_data"] + numeric_cols + text_cols:
        df[col] = df[col].astype(str).fillna("").apply(lambda x: x.strip() if isinstance(x, str) else x)

# === 🔐 JSON-строка в payment_data ===
if "payment_data" in df.columns:
    df["payment_data"] = df["payment_data"].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else str(x))

# === 📊 Подготовка данных для выгрузки ===
values = df.astype(str).where(pd.notnull(df), "").values.tolist()

# === 🔐 Авторизация Google API ===
credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=credentials)
sheet = service.spreadsheets()

# === 📘 Функция: число → буквы (Excel колонки) ===
def colnum_string(n):
    result = ''
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result

# === 🧽 Очистка значений на листе ===
end_col_letter = colnum_string(len(df.columns))
clear_range = f"{SHEET_NAME}!A3:{end_col_letter}"
sheet.values().batchClear(spreadsheetId=SPREADSHEET_ID, body={"ranges": [clear_range]}).execute()
print(f"😹 Удалено значений: {len(values)}")

# === 📅 Обновление данных батчами по 1000 строк ===
if values:
    batch_size = 1000
    total = len(values)
    for i in range(0, total, batch_size):
        chunk = values[i:i+batch_size]
        start_row = 3 + i
        end_col_letter = colnum_string(len(df.columns))
        end_row = start_row + len(chunk) - 1
        data_range = f"{SHEET_NAME}!A{start_row}:{end_col_letter}{end_row}"
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=data_range,
            valueInputOption="USER_ENTERED",
            body={"values": chunk}
        ).execute()
    print(f"✅ Загружено: {total}")
else:
    print("⚠️ Нет данных")

# === 🗓️ Дата в тех заметки ===
note_sheet_range = "тех заметки!D1"
yesterday = (end_date - pd.Timedelta(days=1)).strftime('%d-%m-%Y')
sheet.values().update(
    spreadsheetId=SPREADSHEET_ID,
    range=note_sheet_range,
    valueInputOption="USER_ENTERED",
    body={"values": [[yesterday]]}
).execute()
print(f"📄 Дата выгрузки: {yesterday}")
