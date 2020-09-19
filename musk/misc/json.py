import json
from datetime import datetime


class PythonObjectEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, frozenset):
            return {"_python_frozenset": str(obj)}
        else:
            return super().default(obj)


def as_python_object(dict_):
    if "_python_frozenset" in dict_:
        return eval(dict_["_python_frozenset"])
    return dict_
