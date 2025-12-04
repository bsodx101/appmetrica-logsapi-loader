#!/usr/bin/env python3
"""
  scheduler.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from datetime import datetime, date, time, timedelta
import logging
from time import sleep
from typing import List, Optional, Generator

import pandas as pd

from state import StateStorage, AppIdState
from fields import SchedulingDefinition

logger = logging.getLogger(__name__)


class UpdateRequest(object):
    ARCHIVE = 'archive'
    LOAD_ONE_HOUR = 'load_one_hour'
    LOAD_HOUR_IGNORED = 'load_hour_ignored'

    def __init__(self, source: str, app_id: str, p_hour: Optional[datetime],
                 update_type: str):
        self.source = source
        self.app_id = app_id
        self.hour = p_hour
        self.update_type = update_type


class Scheduler(object):
    ARCHIVED_HOUR = datetime(3000, 1, 1, 0, 0, 0)

    def __init__(self, state_storage: StateStorage,
                 scheduling_definition: SchedulingDefinition,
                 app_ids: List[str], update_limit: timedelta,
                 update_interval: timedelta, fresh_limit: timedelta):
        self._state_storage = state_storage
        self._definition = scheduling_definition
        self._app_ids = app_ids
        self._update_limit = update_limit
        self._update_interval = update_interval
        self._fresh_limit = fresh_limit
        self._state = None

    def _load_state(self):
        self._state = self._state_storage.load()
        # После загрузки state обязательно инициализируем last_profile_update в профилях!
        for app_id_state in getattr(self._state, 'app_id_states', []):
            if app_id_state.app_id.startswith('profiles_'):
                if not hasattr(app_id_state, 'last_profile_update'):
                    app_id_state.last_profile_update = None

    def _save_state(self):
        self._state_storage.save(self._state)

    def _get_or_create_app_id_state(self, source: str, app_id: str) -> AppIdState:
        app_id_state_id = f"{source}_{app_id}"
        app_id_states = [s for s in self._state.app_id_states if s.app_id == app_id_state_id]
        if len(app_id_states) == 0:
            app_id_state = AppIdState(app_id_state_id)
            self._state.app_id_states.append(app_id_state)
        else:
            app_id_state = app_id_states[0]
        return app_id_state

    def _mark_hour_updated(self, app_id_state: AppIdState, p_hour: datetime,
                           now: Optional[datetime] = None):
        if app_id_state.app_id.startswith('profiles_'):
            return
        logger.debug('Data for {} of {} is updated'.format(
            p_hour, app_id_state.app_id
        ))
        app_id_state.date_updates[p_hour] = now or datetime.now()
        self._save_state()

    def _mark_hour_archived(self, app_id_state: AppIdState, p_hour: datetime):
        if app_id_state.app_id.startswith('profiles_'):
            return
        logger.debug('Data for {} of {} is archived'.format(
            p_hour, app_id_state.app_id
        ))
        app_id_state.date_updates[p_hour] = self.ARCHIVED_HOUR
        self._save_state()

    def _is_hour_archived(self, app_id_state: AppIdState, p_hour: datetime):
        updated_at = app_id_state.date_updates.get(p_hour)
        return updated_at is not None and updated_at == self.ARCHIVED_HOUR

    def _finish_updates(self, now: datetime = None):
        logger.debug('Updates are finished')
        self._state.last_update_time = now or datetime.now()
        self._save_state()

    def _wait_time(self, update_interval: timedelta,
                   now: datetime = None) \
            -> Optional[timedelta]:
        if not self._state.last_update_time:
            return None
        now = now or datetime.now()
        delta = self._state.last_update_time - now + update_interval
        if delta.total_seconds() < 0:
            return None
        return delta

    def _wait_if_needed(self):
        wait_time = self._wait_time(self._update_interval)
        if wait_time:
            logger.info('Sleep for {}'.format(wait_time))
            sleep(wait_time.total_seconds())

    def _archive_old_hours(self, app_id_state: AppIdState, app_id: str):
        for p_hour, updated_at in app_id_state.date_updates.items():
            if self._is_hour_archived(app_id_state, p_hour):
                continue
            last_event_hour = p_hour.replace(minute=59, second=59)
            fresh = updated_at - last_event_hour < self._fresh_limit
            if not fresh:
                for source in self._definition.date_required_sources:
                    yield UpdateRequest(source, app_id, p_hour,
                                        UpdateRequest.ARCHIVE)
                self._mark_hour_archived(app_id_state, p_hour)

    def _update_hour(self, app_id_state: AppIdState, app_id: str, p_hour: datetime,
                     started_at: datetime) \
            -> Generator[UpdateRequest, None, None]:
        sources = self._definition.date_required_sources
        updated_at = app_id_state.date_updates.get(p_hour)
        last_event_hour = p_hour.replace(minute=59, second=59)
        if updated_at:
            updated = started_at - updated_at < self._update_interval
            if updated:
                return
        last_event_delta = (updated_at or started_at) - last_event_hour
        for source in sources:
            yield UpdateRequest(source, app_id, p_hour,
                                UpdateRequest.LOAD_ONE_HOUR)
        # УДАЛЕНО: self._mark_hour_updated(app_id_state, p_hour)

        fresh = last_event_delta < self._fresh_limit
        if not fresh:
            for source in sources:
                yield UpdateRequest(source, app_id, p_hour,
                                    UpdateRequest.ARCHIVE)
            self._mark_hour_archived(app_id_state, p_hour)

    def _update_hour_ignored_fields(self, app_id: str):
        for source in self._definition.date_ignored_sources:
            yield UpdateRequest(source, app_id, None,
                                UpdateRequest.LOAD_HOUR_IGNORED)

    def _update_profiles_if_needed(self, app_id: str, now: datetime):
        profiles_app_id_state = self._get_or_create_app_id_state("profiles", app_id)
        # Гарантируем наличие атрибута last_profile_update всегда!
        if not hasattr(profiles_app_id_state, 'last_profile_update'):
            profiles_app_id_state.last_profile_update = None
        from settings import PROFILE_UPDATE_INTERVAL
        last_profile_update = profiles_app_id_state.last_profile_update
        if last_profile_update is None or (now - last_profile_update).total_seconds() > PROFILE_UPDATE_INTERVAL * 3600:
            yield UpdateRequest(
                source="profiles",
                app_id=app_id,
                p_hour=None,
                update_type="load_profiles"
            )
            profiles_app_id_state.last_profile_update = now

    def update_requests(self) \
            -> Generator[UpdateRequest, None, None]:
        self._load_state()
        self._wait_if_needed()
        started_at = datetime.now()
        from settings import SAFE_LAG_HOURS
        for app_id in self._app_ids:
            for source in self._definition.date_required_sources:
                app_id_state = self._get_or_create_app_id_state(source, app_id)
                hour_to = (started_at - timedelta(hours=SAFE_LAG_HOURS)).replace(minute=0, second=0, microsecond=0)
                dt_from = hour_to - self._update_limit
                hour_from = dt_from.replace(hour=0, minute=0, second=0, microsecond=0)

                updates = self._archive_old_hours(app_id_state, app_id)
                for update_request in updates:
                    yield update_request

                for pd_hour in pd.date_range(hour_from, hour_to, freq='H'):
                    p_hour = pd_hour.to_pydatetime().replace(minute=0, second=0, microsecond=0)  # type: datetime
                    updates = self._update_hour(app_id_state, app_id, p_hour, started_at)
                    for update_request in updates:
                        yield update_request

                updates = self._update_hour_ignored_fields(app_id)
                for update_request in updates:
                    yield update_request
        # Добавить профили в tasks (Только одна таска, без почасовых!)
        now = datetime.now()
        for app_id in self._app_ids:
            updates = self._update_profiles_if_needed(app_id, now)
            for update_request in updates:
                # ГАРАНТИРУЕМ только type==load_profiles и hour is None
                if (update_request.source == "profiles"
                      and update_request.update_type == "load_profiles"
                      and update_request.hour is None):
                    yield update_request
        self._finish_updates()
