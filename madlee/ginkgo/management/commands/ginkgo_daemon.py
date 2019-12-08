import logging
from time import sleep, time as now
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django_redis import get_redis_connection

from ....misc.dj import run_forever
from ... import Ginkgo
from ...const import KEY_DAEMON, GINKGO_SEPERATOR
from ...const import CMD_NEW_LEAF, CMD_ENSURE
from ...const import SHA_MISSING, SHA_LOAD
from ...const import MSG_ENSURED



def add_leaf(logger, db, data):
    key, slot, size = data.decode().split(GINKGO_SEPERATOR)
    logger.info('[%s] Add leaf %s %s %s', db.name, key, slot, size)
    try:
        db.add_leaf(key, slot, size)
        return True
    except AssertionError:
        return False


def ensure(logger, db, data):
    data = data.split(GINKGO_SEPERATOR)
    redis = db.redis
    dbname = redis.name
    call_back, start, finish, keys = data[0], data[1], data[2], data[3:]
    sha_missing = db.sha[SHA_MISSING]
    sha_load    = db.sha[SHA_LOAD]
    for code in keys:
        missing = redis.evalsha(sha_missing, 0, 
            dbname, code, start, finish
        )
        blocks = []
        for i in range(0, len(missing), 2):
            m1, m2 = missing[i], missing[i+1]
            blocks += db.get_blocks(code, m1, m2)
        redis.evalsha(sha_load, dbname, code, *blocks)

    redis.publish(call_back, MSG_ENSURED)
        


COMMAND_HANDLES = {
    CMD_NEW_LEAF: add_leaf,
    CMD_ENSURE: ensure
}



@run_forever(5)
def save_back(logger, redis, ginkgo_set, save_gap):
    last_slots = {}
    for dbname, db in ginkgo_set.items():
        for leaf in db.all_leaves.keys():
            last_slots[(dbname, leaf)] = db.last_slot(leaf)

    while True:
        sleep(save_gap)
        for dbname, db in ginkgo_set.items():
            for leaf in db.all_leaves.keys():
                keys = GINKGO_SEPERATOR.join(dbname, leaf, '*')
                keys = redis.keys(keys)
                last = last_slots.get((dbname, leaf), None)
                if last:
                    keys = [key for key in keys if int(key.split(GINKGO_SEPERATOR)[2]) >= last]
                for key in keys:
                    redis.evalsha(SHA_GET_SLOT, 0, )




@run_forever(5)
def main_loop(logger, redis, style, reset_redis, save_gap, ginkgo_set):
    redis = get_redis_connection(redis)
    ginkgo_set = {
        row: Ginkgo(redis, row, False, style)
        for row in ginkgo_set
    }
    if reset_redis:
        for row in ginkgo_set.values():
            ginkgo_set.prepare_redis()

    if save_gap:
        pass

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
        parser.add_argument('--save-gap', type=int, default=None)
        parser.add_argument('db', nargs='+')


    def handle(self, logger, redis, style, reset_redis, save_gap, db, *args, **options):
        logger = logging.getLogger(logger)
        main_loop(logger, redis, style, reset_redis, save_gap, db)

