from collections import namedtuple
from django.db import models
from django.conf import settings
from django.contrib.auth.models import User
from django_redis import get_redis_connection


from .fields import JSONField
from .misc import load_pickle, dump_pickle
from .misc import join_path, is_file, ensure_dirs
from .misc import DateTime
from .const import KEY_REGIA_CACHE



class TimedModel(models.Model):
    '''Mixing class for '''
    created_at = models.DateTimeField(editable=False, blank=True, auto_now_add=True)
    updated_at = models.DateTimeField(editable=False, blank=True, auto_now=True)

    class Meta:
        abstract = True




def natural_model(cls):
    '''A class decorator to enable natural key'''
    class NaturalKeyManager(models.Manager):
        def get_by_natural_key(self, *nk):
            return self.get(**dict(zip(cls.NATURAL_KEY_FILTER, nk)))
    objects = NaturalKeyManager()
    objects.model = cls
    cls.objects = objects
    return cls




@natural_model
class Tag(TimedModel):
    NATURAL_KEY_FILTER = 'name',


    name        = models.CharField(max_length=50, unique=True)


    def natural_key(self):
        return self.name,

    def __str__(self):
        return self.name




class Alert(TimedModel):
    LEVEL_CHOICES = ('I', 'Infomation'), ('W', 'Warning'), ('D', 'Danger'), ('E', 'Emergency')
    STATUS_CHOICES = ('O', 'Openning'), ('N', 'Notified'), ('C', 'Closed'),
    
    REDIS_SHA_UPDATE_ALERT = None


    category    = models.CharField(max_length=4, db_index=True)
    name        = models.CharField(max_length=200)
    note        = models.TextField(blank=True, default='')
    level       = models.CharField(max_length=1, choices=LEVEL_CHOICES)
    status      = models.CharField(max_length=1, choices=STATUS_CHOICES)
    comment     = models.TextField(blank=True, default='')
    closed_by   = models.ForeignKey(User, null=True, blank=True, default=None)

    @property
    def key(self):
        return '%s|%s' % (self.category, self.name)


    def save(self, *args, **kwargs):
        redis_key = settings.REDIS_KEY_ALERT
        redis = get_redis_connection(redis_key)
        if self.id is None and 'pk' not in kwargs and 'id' not in kwargs:
            pk = redis.hget(redis_key+"|PK", self.key)
            if pk:
                self.id = self.pk = int(pk)
        super().save(*args, **kwargs)
        self.update_redis(redis)


    def update_redis(self, redis):
        if Alert.REDIS_SHA_UPDATE_ALERT == None:
            script = settings.REDIS_LUA_UPDATE_ALERT % {'REDIS_KEY': settings.REDIS_KEY_ALERT}
            Alert.REDIS_SHA_UPDATE_ALERT = redis.script_load(script)
        redis.evalsha(0, self.pk, self.category, self.name, self.note, self.level, self.status)
