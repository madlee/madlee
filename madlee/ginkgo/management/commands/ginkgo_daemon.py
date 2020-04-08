import logging
from time import sleep, time as now
from threading import Thread
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django_redis import get_redis_connection

from ....misc.time import SECONDS_IN_A_DAY, DateTime, TimeDelta
from ....misc.dj import run_forever
from ...daemon import GinkgoDaemon
from ...const import GINKGO_SEPERATOR, KEY_DAEMON




@run_forever(5)
def main_loop(logger, redis, style, reset_redis, save_gap, ginkgo_set):
    redis = get_redis_connection(redis)
    ginkgo_set = [
        GinkgoDaemon(redis, row, style)
        for row in ginkgo_set
    ]

    if reset_redis:
        for ginkgo in ginkgo_set:
            scripts = settings.GINGKO_SCRIPTS.get(
                ginkgo.name, settings.DEFALUT_GINKGO_SCRIPTS
            )
            ginkgo.reset_redis(scripts)

        while True:
            next_save = (now() // save_gap + 1) * save_gap
            delta = next_save - now()
            if delta > 0:
                sleep(delta)

            for row in ginkgo_set.values():
                dirties = row.save_dirty()
                logger.info('[%s] Save dirty slots %s.', row.name, ', '.join(dirties))


class Command(BaseCommand):
    help = 'Ginkgo Daemon Server.'


    def add_arguments(self, parser):
        parser.add_argument('--redis', default='ginkgo')
        parser.add_argument('--logger', default='ginkgo')
        parser.add_argument('--style', default=None)
        parser.add_argument('--reset-redis', default=False, action='store_true')
        parser.add_argument('--save-gap', type=int, default=300)
        parser.add_argument('db', nargs='+')


    def handle(self, logger, redis, style, reset_redis, save_gap, db, *args, **options):
        logger = logging.getLogger(logger)
        main_loop(logger, redis, style, reset_redis, save_gap, db)


