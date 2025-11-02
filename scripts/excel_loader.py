import pandas as pd
import argparse
import sys
import os
from sqlalchemy import create_engine, text, inspect
from datetime import datetime
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class ExcelToPostgresLoader:
    def __init__(self, db_url=None):
        if db_url is None:
            db_url = self._get_db_url_from_env()
        
        self.engine = create_engine(db_url)
        self.connection_info = db_url.split('@')[-1]
        logger.info(f"Подключение к БД: {self.connection_info}")
        
    def _get_db_url_from_env(self):
        host = os.getenv('POSTGRES_EXTERNAL_HOST', 'localhost')
        port = os.getenv('POSTGRES_PORT', '5432')
        database = os.getenv('POSTGRES_DB', 'analytics_db')
        user = os.getenv('POSTGRES_USER', 'admin')
        password = os.getenv('POSTGRES_PASSWORD', 'admin')
        
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
    
    def load_all_sheets(self, excel_file, if_exists='replace', schema='public', create_id_column=True):
        try:
            if not os.path.exists(excel_file):
                raise FileNotFoundError(f"Файл {excel_file} не найден")
            
            logger.info(f"Чтение всех листов из {excel_file}")
            
            excel_data = pd.read_excel(excel_file, sheet_name=None)
            logger.info(f"Найдено листов: {len(excel_data)}")
            
            results = {}
            for sheet_name, df in excel_data.items():
                table_name = self._clean_table_name(sheet_name)
                logger.info(f"Загрузка листа '{sheet_name}' как таблица '{table_name}'")
                
                success = self._load_sheet(df, table_name, sheet_name, if_exists, schema, create_id_column)
                results[sheet_name] = success
            
            return results
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке файла: {e}")
            return {}
    
    def _load_sheet(self, df, table_name, sheet_name, if_exists, schema, create_id_column):
        try:
            logger.info(f"Лист '{sheet_name}': {len(df)} строк, {len(df.columns)} колонок")
            
            if create_id_column and 'id' not in df.columns:
                df.reset_index(drop=True, inplace=True)
                df.index += 1
                df['id'] = df.index
            
            start_time = datetime.now()
            
            df.to_sql(
                table_name, 
                self.engine, 
                if_exists=if_exists, 
                index=False,
                schema=schema
            )
            
            load_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"Лист '{sheet_name}' загружен как '{table_name}' за {load_time:.2f} сек")
            
            self._verify_load(table_name, schema, len(df))
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке листа '{sheet_name}': {e}")
            return False
    
    def load_single_sheet(self, excel_file, table_name=None, sheet_name=0, if_exists='replace', schema='public', create_id_column=True):
        try:
            if not os.path.exists(excel_file):
                raise FileNotFoundError(f"Файл {excel_file} не найден")
            
            if table_name is None:
                if isinstance(sheet_name, str):
                    table_name = self._clean_table_name(sheet_name)
                else:
                    table_name = self._clean_table_name(f"sheet_{sheet_name}")
            
            logger.info(f"Загрузка листа {sheet_name} из {excel_file} как таблица {table_name}")
            
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            logger.info(f"Прочитано {len(df)} строк, {len(df.columns)} колонок")
            
            return self._load_sheet(df, table_name, str(sheet_name), if_exists, schema, create_id_column)
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке: {e}")
            return False
    
    def _clean_table_name(self, name):
        import re
        clean_name = re.sub(r'[^\w]', '_', name.lower())
        clean_name = re.sub(r'_+', '_', clean_name)
        clean_name = clean_name.strip('_')
        return clean_name
    
    def _verify_load(self, table_name, schema, expected_count):
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}"))
                actual_count = result.scalar()
                
                inspector = inspect(self.engine)
                columns = inspector.get_columns(table_name, schema=schema)
                
                logger.info(f"Проверка таблицы {table_name}:")
                logger.info(f"  - Ожидалось строк: {expected_count}")
                logger.info(f"  - Загружено строк: {actual_count}")
                logger.info(f"  - Колонки: {[col['name'] for col in columns]}")
                
                if expected_count == actual_count:
                    logger.info(f"✓ Таблица {table_name} успешно загружена")
                else:
                    logger.warning(f"⚠ В таблице {table_name} количество строк не совпадает")
                    
        except Exception as e:
            logger.error(f"Ошибка при проверке таблицы {table_name}: {e}")
    
    def list_tables(self):
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            logger.info(f"Таблицы в базе ({self.connection_info}):")
            for table in tables:
                columns = inspector.get_columns(table)
                logger.info(f"  - {table} ({len(columns)} колонок)")
            return tables
        except Exception as e:
            logger.error(f"Ошибка при получении списка таблиц: {e}")
    
    def get_table_info(self, table_name, schema='public'):
        try:
            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name, schema=schema)
            
            logger.info(f"Информация о таблице {table_name}:")
            for col in columns:
                logger.info(f"  - {col['name']}: {col['type']} (nullable: {col['nullable']})")
                
            return columns
        except Exception as e:
            logger.error(f"Ошибка при получении информации о таблице: {e}")

def main():
    parser = argparse.ArgumentParser(description='Загрузка Excel файлов в PostgreSQL')
    parser.add_argument('excel_file', nargs='?', help='Путь к Excel файлу')
    parser.add_argument('--table', '-t', help='Имя таблицы (для одного листа)')
    parser.add_argument('--sheet', '-s', help='Имя или индекс листа (для одного листа)')
    parser.add_argument('--all-sheets', '-a', action='store_true', help='Загрузить все листы')
    parser.add_argument('--mode', '-m', choices=['replace', 'append', 'fail'], default='replace', help='Режим загрузки')
    parser.add_argument('--schema', default='public', help='Схема БД')
    parser.add_argument('--no-id', action='store_true', help='Не создавать колонку id')
    parser.add_argument('--list-tables', action='store_true', help='Показать список таблиц')
    parser.add_argument('--table-info', help='Показать информацию о таблице')
    
    args = parser.parse_args()
    
    loader = ExcelToPostgresLoader()
    
    if args.list_tables:
        loader.list_tables()
        return
    
    if args.table_info:
        loader.get_table_info(args.table_info)
        return
    
    if not args.excel_file:
        logger.error("Не указан файл для загрузки")
        sys.exit(1)
    
    if args.all_sheets:
        results = loader.load_all_sheets(
            excel_file=args.excel_file,
            if_exists=args.mode,
            schema=args.schema,
            create_id_column=not args.no_id
        )
        
        success_count = sum(1 for result in results.values() if result)
        logger.info(f"Успешно загружено {success_count} из {len(results)} листов")
        sys.exit(0 if success_count == len(results) else 1)
    else:
        success = loader.load_single_sheet(
            excel_file=args.excel_file,
            table_name=args.table,
            sheet_name=args.sheet if args.sheet else 0,
            if_exists=args.mode,
            schema=args.schema,
            create_id_column=not args.no_id
        )
        
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()