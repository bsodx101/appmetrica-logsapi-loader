#!/usr/bin/env python3
"""
  state.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from datetime import datetime
from typing import Optional, List, Dict


class AppIdState(object):
    __slots__ = [
        "app_id",
        "date_updates",  # ключи - начало часа (datetime: 2025-12-03 14:00:00)
        "last_profile_update",  # только для профилей
    ]

    def __init__(self, app_id: str,
                 date_updates: Optional[Dict[datetime, datetime]] = None):
        self.app_id = app_id
        self.date_updates = date_updates or dict()
        # Инициализация last_profile_update для профилей
        if self.app_id.startswith("profiles_"):
            self.last_profile_update = None


class State(object):
    __slots__ = [
        "version",
        "last_update_time",
        "app_id_states",
    ]

    def __init__(self, last_update_time: Optional[datetime] = None,
                 app_id_states: Optional[List[AppIdState]] = None):
        self.version = 1
        self.last_update_time = last_update_time
        self.app_id_states = app_id_states or []  # type: List[AppIdState]
