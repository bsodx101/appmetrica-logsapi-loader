#!/usr/bin/env python3
"""
  json_serialization.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from calendar import timegm
from datetime import datetime
from json import JSONEncoder, JSONDecoder
from typing import Dict, Any

from .state import State, AppIdState

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'  # Часовой слот (пример: 2025-12-03 14:00:00)

def _from_unix_time(u: int) -> datetime:
    return datetime.utcfromtimestamp(u)

def _to_unix_time(dt: datetime) -> int:
    return int(dt.timestamp())

class StateJSONEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return _to_unix_time(o)
        elif isinstance(o, AppIdState):
            date_updates = dict()
            for dt_hour, updated_at in o.date_updates.items():
                # ключ — строка-время начала часа
                date_updates[dt_hour.strftime(DATETIME_FORMAT)] = int(updated_at.timestamp())
            return {
                "app_id": o.app_id,
                "date_updates": date_updates,
            }
        elif isinstance(o, State):
            return {
                "last_update_time": int(o.last_update_time.timestamp()) if o.last_update_time else None,
                "app_id_states": o.app_id_states
            }

def _parse_date_updates(json_object: Dict[str, Any]):
    date_updates = dict()
    for target_hour_str, update_ts in json_object.items():
        dt_hour = datetime.strptime(target_hour_str, DATETIME_FORMAT)
        update_dt = _from_unix_time(update_ts)
        date_updates[dt_hour] = update_dt
    return date_updates

def _parse_app_id_state(json_object: Dict[str, Any]):
    date_updates = _parse_date_updates(json_object["date_updates"])
    return AppIdState(json_object["app_id"], date_updates)

def _parse_state(json_object: Dict[str, Any]):
    last_update_time = None
    if json_object["last_update_time"] is not None:
        last_update_time = _from_unix_time(json_object["last_update_time"])
    app_id_states = []
    if "app_id_states" in json_object.keys():
        app_id_states = map(_parse_app_id_state, json_object["app_id_states"])
    return State(last_update_time, list(app_id_states))

def _hook(json_object):
    if "app_id_states" in json_object:
        return _parse_state(json_object)
    else:
        return json_object

class StateJSONDecoder(JSONDecoder):
    def __init__(self):
        super().__init__(object_hook=_hook)
