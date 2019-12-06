import logging
from time import sleep, time as now
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django_redis import get_redis_connection

from ....misc.dj import run_forever
from ... import Ginkgo
from ...const import KEY_DAEMON, GINKGO_SEPERATOR
from ...const import CMD_NEW_LEAF




def add_leaf(logger, db, data):
    key, slot, size = data.decode().split(GINKGO_SEPERATOR)
    logger.info('[%s] Add leaf %s %s %s', db.name, key, slot, size)
    try:
        db.add_leaf(key, slot, size)
        return True
    except AssertionError:
        return False




COMMAND_HANDLES = {
    CMD_NEW_LEAF: add_leaf
}




@run_forever(5)
def main_loop(logger, redis, style, reset_redis, ginkgo_set):
    redis = get_redis_connection(redis)
    ginkgo_set = {
        row: Ginkgo(redis, row, False, style)
        for row in ginkgo_set
    }
    if reset_redis:
        for row in ginkgo_set.values():
            ginkgo_set.prepare_redis()

    pubsub = redis.pubsub()
    pubsub.psubscribe(GINKGO_SEPERATOR.join(('*', KEY_DAEMON, '*')))
    for item in pubsub.listen():
        if item['type'] == 'pmessage':
            db_name, _, command = item['channel'].decode().split(GINKGO_SEPERATOR)
            try:
                db = ginkgo_set[db_name]
                data = item['data']
                COMMAND_HANDLES[command](logger, db, data)
            except KeyError:
                pass




class Command(BaseCommand):
    help = 'Ginkgo Daemon Server.'


    def add_arguments(self, parser):
        parser.add_argument('--redis', default='ginkgo')
        parser.add_argument('--logger', default='ginkgo')
        parser.add_argument('--style', default=None)
        parser.add_argument('--reset-redis', default=False, action='store_true')
        parser.add_argument('db', nargs='+')


    def handle(self, logger, redis, style, reset_redis, db, *args, **options):
        logger = logging.getLogger(logger)
        main_loop(logger, redis, style, reset_redis, db)
        


    