import dataclasses as dc
from typing import Optional, List, Dict, Any

try:
    # Python 3.8
    from typing import Callable
except ImportError:
    # Python >= 3.9
    from collections.abc import Callable

import iso8601
import pickle
import base64
import json
import six


class Base(object):
    '''Base class for all deserialization data classes

    - `field_map` defines name mappings from source dict to class members
    - `value_map` contains transformation functions for fields that transform source dict values
      to corresponding class member types
    '''

    field_map: Dict[str, str] = {}
    value_map: Dict[str, Callable[[Any], Any]] = {}

    def __init__(self, **kwargs):
        raise RuntimeError(f'Base is not supposed to be used directly, got: {kwargs}')

    @classmethod
    def from_dict(cls, d):
        values: Dict[str, Any] = {}
        for key, value in d.items():
            field: str = cls.field_map.get(key, key)
            try:
                value_map_function = cls.value_map.get(field)
                values[field] = value_map_function(value) if value_map_function else value
            except ValueError:
                raise
            except Exception as e:
                raise ValueError(e)

        return cls(**values)

    @classmethod
    def deserialize(cls, data):
        obj = pickle.loads(base64.decodebytes(data.encode('utf-8')))
        if not isinstance(obj, cls):
            raise ValueError(f'Deserialized data describes a {type(obj)}, expected {cls}')
        return obj

    def serialize(self):
        return base64.encodebytes(pickle.dumps(self)).decode('utf-8')

    # ====== ASSUMPTIONS ======
    # JSON (De)serialization
    # 1. field_map doesn't map FROM valid dataclass field names and doesn't map TO mapped field names
    #    -> set(cls.field_map.keys()).intersection({f.name for f in dc.fields(cls)}) == set()  *for all dataclasses*
    #    -> all(v not in field_map for v in field_map.values())
    # 2. value_map functions accept stringified versions of their target types IF they are not JSON serializable
    #    -> value_function(xroad_data) == value_function(str(value_function(xroad_data)))
    # 3. value_map functions accept None as input if they can output None

    def as_dict(self):
        return dc.asdict(self)

    @classmethod
    def deserialize_json(cls, data):
        obj = json.loads(data)
        return cls.from_dict(obj)

    def serialize_json(self):
        return json.dumps(self.as_dict(), default=str)


# Field mapping functions

def optional(fn):
    '''Modifies the provided function to return None if it receives None'''
    def parse(x):
        return None if x is None else fn(x)
    return parse


def xroad_list_value(key: str, cls):
    def parse(value) -> List[cls]:
        if type(value) is list:
            items = value
        else:
            list_or_dict = (value or {}).get(key) or []
            if type(list_or_dict) is list:
                items = list_or_dict
            elif type(list_or_dict) is dict:
                items = [list_or_dict]
            else:
                # TODO: this defaults missing values to empty list
                items = []
        return [cls.from_dict(item) for item in items]
    return parse


def xroad_service_version_value(v) -> Optional[str]:
    if v is None:
        return v
    elif type(v) in (str, six.text_type):
        return v
    elif type(v) is int:
        return six.text_type('{}.0'.format(v))
    elif type(v) is float:
        return six.text_type(v)
    else:
        raise Exception('Unexpected service version type: {}'.format(repr(type(v))))


date_value = iso8601.parse_date


def class_value(cls):
    return cls.from_dict


def class_list_value(cls):
    def parse(items) -> List[cls]:
        return [cls.from_dict(item) for item in items]
    return parse
