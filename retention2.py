from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv
import os
import time
from collections import defaultdict

# === 🌍 Загрузка переменных окружения ===
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT"))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")

# === 📄 Названия Google Sheets ===
SPREADSHEET_ID = "11f4e2QuKXNWyOjeAYsSDWCUfqP-9yBAmdsBOGwAeny4"
SOURCE_SHEET_NAME = "Исходник"
RETENTION_SUBS_SHEET_NAME = "Retention регулярных подписок"
RETENTION_REGULAR_DONORS_SHEET_NAME = "Retention регулярных доноров (ID)"
RETENTION_ALL_DONORS_SHEET_NAME = "Retention всех доноров (ID)"

# === 🔐 Авторизация Google API ===
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=credentials)
sheet = service.spreadsheets()

def col_letter(n):
    """Преобразует номер колонки (1-based) в буквенное обозначение (A, B, C...)."""
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result

# === 1. SQL-запросы ===
query_regular_payments = f"""
SELECT
    supporter_id,
    payment_type_id,
    subscription_id,
    payment_date,
    amount
FROM public.payment
WHERE is_success = TRUE
  AND subscription_id IS NOT NULL
  AND payment_type_id IN (53, 54, 55, 26, 45)
"""
query_all_payments = f"""
SELECT
    supporter_id,
    payment_type_id,
    subscription_id,
    payment_date,
    amount
FROM public.payment
WHERE is_success = TRUE
  AND payment_type_id IN (53, 54, 55, 26, 45, 1, 16, 24, 34, 50)
"""

# === 2. Подключение и выгрузка из PostgreSQL ===
print("🔌 Подключаюсь к базе данных...")
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
df_regular = pd.read_sql_query(query_regular_payments, engine)
print(f"📥 Загружено {len(df_regular)} регулярных платежей.")
df_all = pd.read_sql_query(query_all_payments, engine)
print(f"📥 Загружено {len(df_all)} всего релевантных платежей.")

# === 3. Предобработка данных ===
today = pd.Timestamp.today()
df_regular["payment_date"] = pd.to_datetime(df_regular["payment_date"], errors='coerce')
df_regular = df_regular[df_regular["payment_date"].dt.to_period("M") < today.to_period("M")].copy()
df_regular["amount"] = pd.to_numeric(df_regular["amount"], errors="coerce").fillna(0)
df_regular["period"] = df_regular["payment_date"].dt.to_period("M")
df_all["payment_date"] = pd.to_datetime(df_all["payment_date"], errors='coerce')
df_all = df_all[df_all["payment_date"].dt.to_period("M") < today.to_period("M")].copy()
df_all["amount"] = pd.to_numeric(df_all["amount"], errors="coerce").fillna(0)
df_all["period"] = df_all["payment_date"].dt.to_period("M")


# ==============================================================================
# ### БЛОК 1: РАСЧЕТ RETENTION РЕГУЛЯРНЫХ ПОДПИСОК (по subscription_id) ###
# ==============================================================================
print("\n--- 🚀 Расчет 1: Retention регулярных подписок ---")
first_payments_subs = df_regular.groupby("subscription_id")["payment_date"].min().dt.to_period("M")
df_regular["start_period_subs"] = df_regular["subscription_id"].map(first_payments_subs)
retention_data_subs = defaultdict(lambda: defaultdict(lambda: {"count": 0, "amount": 0}))
for _, row in df_regular.iterrows():
    start = row["start_period_subs"]
    actual = row["period"]
    if pd.isna(start) or pd.isna(actual): continue
    offset = (actual.year - start.year) * 12 + (actual.month - start.month)
    if offset >= 0:
        retention_data_subs[start][offset]["count"] += 1
        retention_data_subs[start][offset]["amount"] += row["amount"]
periods_subs = sorted(retention_data_subs.keys())
if periods_subs:
    # Определяем глобальный max_offset только для очистки листа
    max_offset_global = max(max(x.keys()) for x in retention_data_subs.values())
    start_row = 4
    updates_subs = []
    for i, start_month in enumerate(periods_subs):
        base_col = 2 + i * 3
        # ***ИЗМЕНЕНИЕ:*** Определяем максимальный offset для КОНКРЕТНОЙ когорты
        cohort_max_offset = max(retention_data_subs.get(start_month, {0:0}).keys())
        
        # ***ИЗМЕНЕНИЕ:*** Цикл идет до личного максимума когорты, а не до глобального
        for offset in range(cohort_max_offset + 1):
            row = start_row + offset
            count = retention_data_subs[start_month][offset]["count"]
            amount = retention_data_subs[start_month][offset]["amount"]
            count_cell, percent_cell, amount_cell = col_letter(base_col) + str(row), col_letter(base_col + 1) + str(row), col_letter(base_col + 2) + str(row)
            formula = f"={count_cell}/{col_letter(base_col)}{start_row}" if offset > 0 else "100%"
            updates_subs.append({"range": f"{RETENTION_SUBS_SHEET_NAME}!{count_cell}:{amount_cell}", "values": [[count, formula, round(amount)]]})
    
    clear_range_subs = f"{RETENTION_SUBS_SHEET_NAME}!B4:ZZ{start_row + max_offset_global + 5}"
    sheet.values().clear(spreadsheetId=SPREADSHEET_ID, range=clear_range_subs).execute()
    sheet.values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"valueInputOption": "USER_ENTERED", "data": updates_subs}).execute()
    print(f"✅ Таблица '{RETENTION_SUBS_SHEET_NAME}' успешно обновлена.")
else:
    print(f"⚠️ Нет данных для расчета '{RETENTION_SUBS_SHEET_NAME}'.")


# ==============================================================================
# ### БЛОК 2: РАСЧЕТ RETENTION РЕГУЛЯРНЫХ ДОНОРОВ (ID) - БЕЗ СУММЫ ###
# ==============================================================================
print("\n--- 🚀 Расчет 2: Retention регулярных доноров (ID) ---")
df_first_donations = df_regular[df_regular['payment_type_id'] == 55]
first_payments_donors = df_first_donations.groupby("supporter_id")["payment_date"].min().dt.to_period("M")
df_regular["start_period_donors"] = df_regular["supporter_id"].map(first_payments_donors)
unique_ids_per_offset_donors = defaultdict(set)
for _, row in df_regular.iterrows():
    start = row["start_period_donors"]
    actual = row["period"]
    if pd.isna(start) or pd.isna(actual): continue
    offset = (actual.year - start.year) * 12 + (actual.month - start.month)
    if offset >= 0:
        unique_ids_per_offset_donors[(start, offset)].add(row["supporter_id"])
retention_data_donors = defaultdict(lambda: defaultdict(int))
for (start, offset), ids in unique_ids_per_offset_donors.items():
    retention_data_donors[start][offset] = len(ids)
periods_donors = sorted(retention_data_donors.keys())
if periods_donors:
    max_offset_global = max(max(x.keys()) for x in retention_data_donors.values())
    start_row = 4
    updates_donors = []
    for i, start_month in enumerate(periods_donors):
        base_col = 2 + i * 2
        base_count = retention_data_donors[start_month].get(0, 0)
        if base_count == 0: continue
        
        cohort_max_offset = max(retention_data_donors.get(start_month, {0:0}).keys())
        for offset in range(cohort_max_offset + 1):
            row = start_row + offset
            count = retention_data_donors[start_month].get(offset, 0)
            count_cell, percent_cell = col_letter(base_col) + str(row), col_letter(base_col + 1) + str(row)
            formula = f"={count_cell}/{base_count}" if offset > 0 else "100%"
            updates_donors.append({"range": f"{RETENTION_REGULAR_DONORS_SHEET_NAME}!{count_cell}:{percent_cell}", "values": [[count, formula]]})
    
    last_col_num = 2 + (len(periods_donors) - 1) * 2 + 1
    clear_range_donors = f"{RETENTION_REGULAR_DONORS_SHEET_NAME}!B4:{col_letter(last_col_num)}{start_row + max_offset_global + 20}"
    sheet.values().clear(spreadsheetId=SPREADSHEET_ID, range=clear_range_donors).execute()
    sheet.values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"valueInputOption": "USER_ENTERED", "data": updates_donors}).execute()
    print(f"✅ Таблица '{RETENTION_REGULAR_DONORS_SHEET_NAME}' успешно обновлена.")
else:
    print(f"⚠️ Нет данных для расчета '{RETENTION_REGULAR_DONORS_SHEET_NAME}'.")

    
# ==============================================================================
# ### БЛОК 3: РАСЧЕТ RETENTION ВСЕХ ДОНОРОВ (ID) - БЕЗ СУММЫ ###
# ==============================================================================
print("\n--- 🚀 Расчет 3: Retention всех доноров (ID) ---")
df_first_events = df_all[df_all['payment_type_id'].isin([16, 55])]
first_payments_all = df_first_events.groupby("supporter_id")["payment_date"].min().dt.to_period("M")
df_all["start_period_all"] = df_all["supporter_id"].map(first_payments_all)
unique_ids_per_offset_all = defaultdict(set)
for _, row in df_all.iterrows():
    start = row["start_period_all"]
    actual = row["period"]
    if pd.isna(start) or pd.isna(actual): continue
    offset = (actual.year - start.year) * 12 + (actual.month - start.month)
    if offset >= 0:
        unique_ids_per_offset_all[(start, offset)].add(row["supporter_id"])
retention_data_all = defaultdict(lambda: defaultdict(int))
for (start, offset), ids in unique_ids_per_offset_all.items():
    retention_data_all[start][offset] = len(ids)
periods_all = sorted(retention_data_all.keys())
if periods_all:
    max_offset_global = max(max(x.keys()) for x in retention_data_all.values())
    start_row = 4
    updates_all = []
    for i, start_month in enumerate(periods_all):
        base_col = 2 + i * 2
        base_count = retention_data_all[start_month].get(0, 0)
        if base_count == 0: continue
        
        cohort_max_offset = max(retention_data_all.get(start_month, {0:0}).keys())
        for offset in range(cohort_max_offset + 1):
            row = start_row + offset
            count = retention_data_all[start_month].get(offset, 0)
            count_cell, percent_cell = col_letter(base_col) + str(row), col_letter(base_col + 1) + str(row)
            formula = f"={count_cell}/{base_count}" if offset > 0 else "100%"
            updates_all.append({"range": f"{RETENTION_ALL_DONORS_SHEET_NAME}!{count_cell}:{percent_cell}", "values": [[count, formula]]})
    
    last_col_num = 2 + (len(periods_all) - 1) * 2 + 1
    clear_range_all = f"{RETENTION_ALL_DONORS_SHEET_NAME}!B4:{col_letter(last_col_num)}{start_row + max_offset_global + 20}"
    sheet.values().clear(spreadsheetId=SPREADSHEET_ID, range=clear_range_all).execute()
    sheet.values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"valueInputOption": "USER_ENTERED", "data": updates_all}).execute()
    print(f"✅ Таблица '{RETENTION_ALL_DONORS_SHEET_NAME}' успешно обновлена.")
else:
    print(f"⚠️ Нет данных для расчета '{RETENTION_ALL_DONORS_SHEET_NAME}'.")


# ==============================================================================
# ### БЛОК 4: ОБНОВЛЕНИЕ ВКЛАДКИ "ИСХОДНИК" ###
# ==============================================================================
print("\n--- 📝 Обновляю вкладку 'Исходник' ---")
df_source_output = df_all.sort_values(by="payment_date", ascending=False)
df_source_output["Месяц"] = df_source_output["payment_date"].dt.month
df_source_output["Год"] = df_source_output["payment_date"].dt.year
df_output = df_source_output[["supporter_id", "payment_type_id", "subscription_id", "payment_date", "amount", "Месяц", "Год"]].copy()
df_output["payment_date"] = pd.to_datetime(df_output["payment_date"], errors='coerce').dt.strftime('%d.%m.%Y')
df_output["amount"] = pd.to_numeric(df_output["amount"], errors='coerce').fillna(0).round(2).apply(lambda x: str(x).replace('.', ','))
clear_range = f"{SOURCE_SHEET_NAME}!A2:H"
max_retries = 3
for attempt in range(max_retries):
    try:
        sheet.values().clear(spreadsheetId=SPREADSHEET_ID, range=clear_range).execute()
        print("🧹 Старые данные из 'Исходника' удалены.")
        break
    except Exception as e:
        if "503" in str(e) and attempt < max_retries - 1:
            print(f"⚠️ Ошибка 503 при очистке 'Исходника'. Повтор через 5 секунд... (попытка {attempt + 1})")
            time.sleep(5)
        else:
            raise
values = df_output.astype(str).where(pd.notnull(df_output), '').values.tolist()
if values:
    body = {"values": values}
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SOURCE_SHEET_NAME}!A2",
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()
    print(f"✅ В 'Исходник' загружено строк: {len(values)}")
else:
    print("⚠️ Нет данных для загрузки в 'Исходник'.")

print("\n🎉 Все задачи успешно выполнены!")