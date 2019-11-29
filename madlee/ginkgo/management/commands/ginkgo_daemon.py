import logging
from time import sleep, time as now
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django_redis import get_redis_connection

from ...misc.dj import run_forever
from .. import Ginkgo


@run_forever(5)
def main_loop(logger, redis, style, db):
    redis = get_redis_connection(redis)
    db = {
        row: Ginkgo(redis, row, False, style)
        for row in db
    }
    pubsub = redis.pubsub()
    pubsub.psubscribe('*|GINKGO-RPC')
    for item in pubsub:
        pass



class Command(BaseCommand):
    help = 'Ginkgo Daemon Server.'


    def add_arguments(self, parser):
        parser.add_argument('--redis', default='ginkgo')
        parser.add_argument('--logger', default='ginkgo')
        parser.add_argument('--style', default=None)
        parser.add_argument('db', nargs='+')


    def handle(self, logger, redis, style, db, *args, **options):
        logger = logging.getLogger(logger)
        main_loop(logger, redis, style, db)
        


    