import pandas as pd
from dotenv import load_dotenv
import os
import logging
from sqlalchemy import create_engine, text
import re

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_table_name(name):
    clean_name = re.sub(r'[^\w]', '_', name.lower())
    clean_name = re.sub(r'_+', '_', clean_name)
    return clean_name.strip('_')

def create_table_from_dataframe(engine, df, table_name):
    try:
        with engine.connect() as conn:
            columns = []
            for col_name, col_type in df.dtypes.items():
                sql_type = 'TEXT'
                if pd.api.types.is_numeric_dtype(col_type):
                    sql_type = 'NUMERIC'
                elif pd.api.types.is_datetime64_any_dtype(col_type):
                    sql_type = 'TIMESTAMP'
                col_name_clean = clean_table_name(str(col_name))
                columns.append(f'"{col_name_clean}" {sql_type}')
            
            columns_sql = ', '.join(columns)
            create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_sql})'
            conn.execute(text(create_sql))
            conn.commit()
            logger.info(f"Таблица {table_name} создана успешно")
            return True
            
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы {table_name}: {e}")
        return False

def insert_data_to_table(engine, df, table_name):
    try:
        df_clean = df.copy()
        df_clean.columns = [clean_table_name(str(col)) for col in df.columns]
        df_clean.to_sql(table_name, engine, if_exists='replace', index=False)
        logger.info(f"Данные загружены в таблицу {table_name}: {len(df)} строк")
        return True
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных в {table_name}: {e}")
        return False

def main():
    try:
        load_dotenv()
        DATABASE_URL = os.getenv("DATABASE_URL")
        if DATABASE_URL is None or not DATABASE_URL:
            raise ValueError("No DATABASE_URL was given in the env")
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            logger.info(f"Подключение к БД: {result.scalar()}")

        excel_data = pd.read_excel('data/data.xlsx', sheet_name=None)
        logger.info(f"Найдено листов: {len(excel_data)}")
        for sheet_name, df in excel_data.items():
            table_name = clean_table_name(sheet_name)
            logger.info(f"Обработка листа '{sheet_name}' -> таблица '{table_name}'")
            logger.info(f"Размер данных: {len(df)} строк, {len(df.columns)} колонок")

            if create_table_from_dataframe(engine, df, table_name):
                insert_data_to_table(engine, df, table_name)
            else:
                logger.error(f"Не удалось создать таблицу {table_name}")

        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """))
            tables = [row[0] for row in result]
            logger.info(f"Созданные таблицы: {tables}")
    except Exception as e:
        logger.exception(f"Ошибка в main: {e}")
        return None

if __name__ == "__main__":
    main()