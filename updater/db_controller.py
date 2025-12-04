    def _load_profiles(self, app_id, db_controller):
        import logging
        logger = logging.getLogger(__name__)
        tmp_suffix = 'tmp'
        latest_suffix = 'latest'
        tmp_table = db_controller.table_name(tmp_suffix)
        latest_table = db_controller.table_name(latest_suffix)
        try:
            logger.info(f"[PROFILES] Start loading fresh snapshot for app_id={app_id}")
            db_controller._db.drop_table(tmp_table)
            db_controller._create_table(tmp_table)
            profiles_df = self.get_profiles_dataframe(app_id)
            if profiles_df is None or profiles_df.empty:
                logger.error(f"[PROFILES] No DataFrame for {app_id}!")
                return
            logger.info(f"[PROFILES] Fetch complete: {len(profiles_df)} rows, columns: {profiles_df.columns.tolist()}")
            db_controller.insert_data(profiles_df, tmp_suffix)
            db_controller._db.drop_table(latest_table)
            db_controller._db.rename_table(tmp_table, latest_table)
            logger.info(f"[PROFILES] Done: loaded and updated latest snapshot for app_id={app_id}, rows: {len(profiles_df)}")
        except Exception as e:
            logger.error(f"[PROFILES] FAILED snapshot load for app_id={app_id}: {e}")
            try:
                db_controller._db.drop_table(tmp_table)
            except Exception as ee:
                logger.warning(f"[PROFILES] Also failed to drop temp table {tmp_table}: {ee}")

import pandas as pd
import requests
import io
from settings import TOKEN

def get_profiles_dataframe(self, app_id):
    url = "https://api.appmetrica.yandex.ru/logs/v1/export/profiles.json"
    params = {
        "application_id": app_id,
        "fields": "appmetrica_device_id,№ карты лояльности,Номер телефона",
        "date_dimension": "default",
        "use_utf8_bom": "true"
    }
    headers = {
        "Authorization": f"OAuth {TOKEN}"
    }
    resp = requests.get(url, params=params, headers=headers)
    resp.raise_for_status()
    df = pd.read_json(io.StringIO(resp.text))
    df = df.rename(columns={
        "appmetrica_device_id": "DeviceID",
        "№ карты лояльности": "LoyaltyCardNumber",
        "Номер телефона": "PhoneNumber"
    })
    return df

"""
  db_controller.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
import logging

from pandas import DataFrame

from db import Database
from fields import DbTableDefinition

logger = logging.getLogger(__name__)

# TODO: Allow customizing
_escape_characters = {
    '\b': '\\\\b',
    '\r': '\\\\r',
    '\f': '\\\\f',
    '\n': '\\\\n',
    '\t': '\\\\t',
    '\0': '\\\\0',
    '\'': '\\\\\'',
    '\\\\': '\\\\\\\\',
}


class DbController(object):
    ARCHIVE_SUFFIX = 'old'
    ALL_SUFFIX = 'all'
    LATEST_SUFFIX = 'latest'

    def __init__(self, db: Database, definition: DbTableDefinition):
        self._db = db
        self._definition = definition

    def table_name(self, suffix: str):
        return '{}_{}'.format(self._definition.table_name, suffix)

    @property
    def merge_re(self):
        return '^{}.*'.format(self._definition.table_name)

    @property
    def date_field(self):
        return self._definition.date_field

    @property
    def sampling_field(self):
        return self._definition.sampling_field

    @property
    def primary_keys(self):
        return self._definition.primary_keys

    def _prepare_db(self):
        if not self._db.database_exists():
            self._db.create_database()
            logger.info('Database "{}" created'.format(self._db.db_name))

    def _prepare_table(self):
        table_name = self.table_name(self.ALL_SUFFIX)
        table_exists = self._db.table_exists(table_name)
        if not table_exists:
            # Теперь схема формируется по export_fields_obj (db_name/db_type) для source
            mini_field_types = [(field.db_name, field.db_type) for field in self._definition.export_fields_obj]
            self._db.create_merge_table(
                table_name,
                mini_field_types,
                self.merge_re
            )

    def prepare(self):
        self._prepare_db()
        self._prepare_table()

    def _fetch_export_fields(self, df: DataFrame) -> DataFrame:
        logger.debug("Fetching exporting fields")
        return df[self._definition.export_fields]

    def _escape_data(self, df: DataFrame) -> DataFrame:
        logger.debug("Escaping symbols")
        escape_chars = dict()
        string_cols = list(df.select_dtypes(include=['object']).columns)
        for col, type in self._definition.column_types.items():
            if type == 'String' and col in string_cols:
                escape_chars[col] = _escape_characters
        df.replace(escape_chars, regex=True, inplace=True)
        return df

    @staticmethod
    def _export_data_to_tsv(df: DataFrame) -> str:
        logger.debug("Exporting data to csv")
        return df.to_csv(index=False, sep='\t')

    def _create_table(self, table_name):
        # Используем имена db_name и db_type для export_fields_obj
        columns_sql = ",\n    ".join([f"{field.db_name} {field.db_type}" for field in self._definition.export_fields_obj])

        partition_candidates = [field.db_name for field in self._definition.export_fields_obj if field.db_type.startswith('DateTime') or field.db_type.startswith('Date')]
        if partition_candidates:
            partition_field = partition_candidates[0]
            partition_sql = f"PARTITION BY toYYYYMM({partition_field})"
        else:
            # Если нет ни одного Date/DateTime - безопасная заглушка!
            partition_sql = "PARTITION BY tuple()"

        order_fields = self._definition.primary_keys if self._definition.primary_keys else (partition_candidates[:1] if partition_candidates else [])
        allowed_names = [field.db_name for field in self._definition.export_fields_obj]
        order_sql = ", ".join([col for col in order_fields if col in allowed_names])
        create_sql = f'''
            CREATE TABLE {self._db.db_name}.{table_name} (
                {columns_sql}
            )
            ENGINE = MergeTree()
            {partition_sql}
            ORDER BY ({order_sql if order_sql else (partition_candidates[0] if partition_candidates else '')})
        '''
        self._db._query_clickhouse(create_sql)

    def _ensure_table_created(self, table_name):
        if not self._db.table_exists(table_name):
            self._create_table(table_name)

    def archive_table(self, table_suffix: str):
        source_table_name = self.table_name(table_suffix)
        if not self._db.table_exists(source_table_name):
            logger.warning('Table to archive is not exist: {}'.format(
                source_table_name
            ))
            return
        archive_table_name = self.table_name(self.ARCHIVE_SUFFIX)
        self._ensure_table_created(archive_table_name)
        self._db.copy_data(source_table_name, archive_table_name)
        self._db.drop_table(source_table_name)

    def recreate_table(self, table_suffix: str):
        table_name = self.table_name(table_suffix)
        self._db.drop_table(table_name)
        self._create_table(table_name)

    def ensure_table_created(self, table_suffix: str):
        table_name = self.table_name(table_suffix)
        self._ensure_table_created(table_name)

    def insert_data(self, df: DataFrame, table_suffix: str):
        # Универсальный rename для всех sources, включая profiles
        rename_dict = {field.load_name: field.db_name for field in getattr(self._definition, 'export_fields_obj', [])}
        df = df.rename(columns=rename_dict)
        required_columns = [field.db_name for field in getattr(self._definition, 'export_fields_obj', []) if field.db_name in df.columns]
        df = df[required_columns]
        logger.info(f'BEFORE INSERT: DataFrame shape: {df.shape}')
        logger.info(f'BEFORE INSERT: Columns: {df.columns.tolist()}')
        if not df.empty:
            logger.info(f'BEFORE INSERT: Head:\n{df.head(3)}')
        else:
            logger.warning("BEFORE INSERT: DataFrame for insert is EMPTY!")
        tsv = self._export_data_to_tsv(df)
        table_name = self.table_name(table_suffix)
        self._db.insert(table_name, tsv)
        try:
            row_count = int(self._db._query_clickhouse(f"SELECT count() FROM {self._db.db_name}.{table_name}").strip())
            logger.info(f"Inserted {len(df)} rows into {table_name}, total rows now: {row_count}")
        except Exception as e:
            logger.warning(f"Could not fetch row count for {table_name}: {e}")

