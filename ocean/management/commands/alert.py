import logging
from time import sleep, time as now
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django_redis import get_redis_connection

class Command(BaseCommand):
    help = 'Alert on weixin.'


    def handle(self, *args, **options):
        redis_key = settings.REDIS_KEY_ALERT
        redis = get_redis_connection(redis_key)
        logger = logging.getLogger('alert')

        while True:
            ts = now()

            
