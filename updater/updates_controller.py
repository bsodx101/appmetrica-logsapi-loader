#!/usr/bin/env python3
"""
  updates_controller.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
import datetime
import logging
import time
from typing import Optional

from fields import SourcesCollection, ProcessingDefinition, LoadingDefinition
from .scheduler import Scheduler, UpdateRequest
from .db_controller import DbController
from .updater import Updater
from .db_controllers_collection import DbControllersCollection

logger = logging.getLogger(__name__)


class UpdatesController(object):
    def __init__(self, scheduler: Scheduler, updater: Updater,
                 sources_collection: SourcesCollection,
                 db_controllers_collection: DbControllersCollection):
        self._scheduler = scheduler
        self._updater = updater
        self._sources_collection = sources_collection
        self._db_controllers_collection = db_controllers_collection

    def _load_into_table(self, app_id: str, hour: Optional[datetime.datetime],
                         table_suffix: str,
                         processing_definition: ProcessingDefinition,
                         loading_definition: LoadingDefinition,
                         db_controller: DbController):
        logger.info('Loading "{hour}" into "{suffix}" of "{source}" '
                    'for "{app_id}"'.format(
            hour=hour or 'latest',
            source=loading_definition.source_name,
            app_id=app_id,
            suffix=table_suffix
        ))
        self._updater.update(app_id, hour, table_suffix, db_controller,
                             processing_definition, loading_definition)

    def _archive(self, source: str, app_id: str, hour: datetime.datetime,
                 table_suffix: str, db_controller: DbController):
        logger.info('Archiving "{hour}" of "{source}" for "{app_id}"'.format(
            hour=hour,
            source=source,
            app_id=app_id
        ))
        db_controller.archive_table(table_suffix)

    def _update(self, update_request: UpdateRequest):
        logger.info(
            f"UPDATE: type={update_request.update_type}, source={update_request.source}, "
            f"app_id={update_request.app_id}, hour={getattr(update_request, 'hour', None)}"
        )
        source = update_request.source
        app_id = update_request.app_id
        hour = update_request.hour
        update_type = update_request.update_type
        if hour is not None:
            table_suffix = '{}_{}'.format(app_id, hour.strftime('%Y%m%d%H'))
        else:
            table_suffix = '{}_{}'.format(app_id, DbController.LATEST_SUFFIX)

        loading_definition = \
            self._sources_collection.loading_definition(source)
        processing_definition = \
            self._sources_collection.processing_definition(source)
        db_controller = \
            self._db_controllers_collection.db_controller(source)

        try:
            if update_type == UpdateRequest.LOAD_ONE_HOUR:
                if source != "profiles":
                    self._load_into_table(app_id, hour, table_suffix,
                                          processing_definition, loading_definition,
                                          db_controller)
                    # Сохраняем состояние только для событий и сессий (НЕ profiles)
                    if source in ("events", "sessions_starts"):
                        app_id_state = self._scheduler._get_or_create_app_id_state(source, app_id)
                        self._scheduler._mark_hour_updated(app_id_state, hour)
            elif update_type == UpdateRequest.ARCHIVE:
                if source != "profiles":
                    self._archive(source, app_id, hour, table_suffix, db_controller)
            elif update_type == UpdateRequest.LOAD_HOUR_IGNORED:
                if source != "profiles":
                    self._load_into_table(app_id, None, table_suffix,
                                          processing_definition, loading_definition,
                                          db_controller)
            elif update_type == "load_profiles":
                self._load_profiles(app_id, db_controller)
            logger.info(
                f"SUCCESS: {update_type} for app_id={app_id}, source={source}, hour={hour}"
            )
        except Exception as e:
            logger.warning(
                f"Exception during update {update_type} for app_id={app_id}, source={source}, hour={hour}: {e}"
            )
            raise

    def _load_profiles(self, app_id, db_controller):
        """
        Загружает актуальный snapshot профиля в ClickHouse:
        - Загружает данные во временную таблицу profiles_<app_id>_tmp;
        - Если всё прошло успешно, atomically переставляет таблицы:
          временную -> latest (устаревшую удаляет).
        - Если что-то не так — не затрагивает актуальную таблицу.
        """
        import logging
        logger = logging.getLogger(__name__)
        tmp_suffix = 'tmp'
        latest_suffix = 'latest'
        tmp_table = db_controller.table_name(tmp_suffix)
        latest_table = db_controller.table_name(latest_suffix)
        try:
            # Шаг 1. Удалить temp-таблицу, если вдруг она осталась
            db_controller._db.drop_table(tmp_table)
            # Шаг 2. Создать temp-таблицу по нужной схеме
            db_controller._create_table(tmp_table)
            # Шаг 3. Загрузить профили в DataFrame (предполагается, что у вас есть функция)
            profiles_df = db_controller.load_profiles_df(app_id) if hasattr(db_controller, 'load_profiles_df') else None
            assert profiles_df is not None, 'profiles_df must be fetched!'
            db_controller.insert_data(profiles_df, tmp_suffix)
            # Шаг 4. После успеха — удалить old и переименовать temp
            db_controller._db.drop_table(latest_table)
            db_controller._db.rename_table(tmp_table, latest_table)
            logger.info(f"Profiles for {app_id} successfully swapped to latest.")
        except Exception as e:
            logger.error(f"Failed to update profiles table for {app_id}: {e}")
            try:
                db_controller._db.drop_table(tmp_table)
            except Exception as ee:
                logger.warning(f"Also failed to drop temp table {tmp_table}: {ee}")

    def _step(self):
        update_requests = self._scheduler.update_requests()
        for update_request in update_requests:
            self._update(update_request)

    def run(self):
        logger.info("Starting updating loop")
        while True:
            try:
                self._step()
            except Exception as e:
                logger.warning(e)
                time.sleep(10)
