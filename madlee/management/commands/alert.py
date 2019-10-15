import logging
from time import sleep, time as now
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django_redis import get_redis_connection

from ...dj_misc import run_forever
from ...weixin import QiyeWeixin


class Command(BaseCommand):
    help = 'Alert on weixin.'


    def handle(self, *args, **options):
        redis_key = settings.REDIS_KEY_ALERT
        redis = get_redis_connection(redis_key)
        logger = logging.getLogger('alert')
        self.weixin = {}


    @run_forever(settings.GAP_ALERT_CHECK)
    def main_loop(self, logger, redis):
        redis_key = settings.REDIS_KEY_ALERT
        gap = settings.GAP_ALERT_CHECK
        format = settings.FORMAT_ALERT_TEXT

        sha = redis.scriptload(settings.REDIS_LUA_GET_ALERT)

        while True:
            ts = now()
            alerts = redis.evalsha(sha, 1, redis_key, ts)
            weixin_set = set()
            for i in range(0, len(alerts), 2):
                key, val = alerts[i], alerts[i+1]
                category, name = key.split('|', 1)
                alert = {
                    'category': category,
                    'name': name,
                    'level': val[0],
                    'status': val[1],
                    'note': val[2:],
                }
                weixin = self.get_weixin(category)
                weixin.push(format % alert)
                weixin_set.add(weixin)

            for weixin in weixin_set:
                weixin.commit()

            delta = ts + gap - now()
            if delta > 0:
                sleep(delta)


    def get_weixin(self, key):
        if key not in self.weixin:
            weixin = settings.ALERT_WEIXIN.get(key, settings.ALERT_WEIXIN['default'])
            self.weixin[key] = QiyeWeixin(*weixin)
        return self.weixin[key]

            
            
