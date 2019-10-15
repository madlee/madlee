# -*- coding: utf-8 -*-
"""
JSONField automatically serializes most Python terms to JSON data.
Creates a TEXT field with a default value of "{}".  See test_json.py for
more information.
 from django.db import models
 from django_extensions.db.fields import json
 class LOL(models.Model):
     extra = json.JSONField()
"""

import json
from abc import ABC, abstractmethod
import django
from django.conf import settings
from django.core.serializers.json import DjangoJSONEncoder
from django.db import models




class TextBasedField(models.TextField):
    """Base mixing for are specialized text/char fields"""

    def to_python(self, value):
        """Convert our string value to list of transactions"""
        if value is None or value == '':
            return self.empty_value()

        if isinstance(value, str):
            res = self.loads(value)
        else:
            res = value

        return res


    def get_prep_value(self, value):
        if not isinstance(value, str):
            return self.dumps(value)
        return super().get_prep_value(value)


    def from_db_value(self, value, expression, connection):  # type: ignore
        return self.to_python(value)


    def get_db_prep_save(self, value, connection, **kwargs):
        """Convert our JSON object to a string before we save"""
        if not isinstance(value, str):
            value = self.dumps(value)

        return value


    @abstractmethod
    def loads(self, value):
        '''load string to proper python object'''
        pass


    @abstractmethod
    def dumps(self, value):
        '''Convert object to string'''
        pass


    @abstractmethod
    def empty_value(self):
        '''An empty value'''
        pass




class CharBasedField(models.CharField):
    """Base mixing for are specialized text/char fields"""

    def to_python(self, value):
        """Convert our string value to list of transactions"""
        if value is None or value == '':
            return self.empty_value()

        if isinstance(value, str):
            res = self.loads(value)
        else:
            res = value

        return res


    def get_prep_value(self, value):
        if not isinstance(value, str):
            return self.dumps(value)
        
        return super().get_prep_value(value)


    def from_db_value(self, value, expression, connection):  # type: ignore
        return self.to_python(value)


    def get_db_prep_save(self, value, connection, **kwargs):
        """Convert our JSON object to a string before we save"""
        if not isinstance(value, str):
            value = self.dumps(value)

        return value


    @abstractmethod
    def loads(self, value):
        '''load string to proper python object'''
        pass


    @abstractmethod
    def dumps(self, value):
        '''Convert object to string'''
        pass


    @abstractmethod
    def empty_value(self):
        '''An empty value'''
        pass



#######################################################################
## JSON Field

def dump_json(value):
    return DjangoJSONEncoder().encode(value)

def load_json(txt):
    value = json.loads(
        txt,
        encoding=settings.DEFAULT_CHARSET
    )
    return value


class JSONDict(dict):
    """
    Hack so repr() called by dumpdata will output JSON instead of
    Python formatted data.  This way fixtures will work!
    """
    def __repr__(self):
        return dump_json(self)


class JSONList(list):
    """
    As above
    """
    def __repr__(self):
        return dump_json(self)


class JSONField(TextBasedField):
    """JSONField is a generic textfield that neatly serializes/unserializes
    JSON objects seamlessly.  Main thingy must be a dict object."""

    def __init__(self, *args, **kwargs):
        kwargs['default'] = kwargs.get('default', dict)
        models.TextField.__init__(self, *args, **kwargs)


    def get_default(self):
        if self.has_default():
            default = self.default

            if callable(default):
                default = default()

            return self.to_python(default)
        return super().get_default()


    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        if self.default == '{}':
            del kwargs['default']
        return name, path, args, kwargs


    def empty_value(self):
        return {}

    def loads(self, value):
        return load_json(value)

    def dumps(self, value):
        return dump_json(value)

## JSON Field
#######################################################################
