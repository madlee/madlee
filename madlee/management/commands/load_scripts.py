import logging
from django.core.management.base import BaseCommand, CommandError
from django_redis import get_redis_connection
from ...misc.file import list_dir, join_path




class Command(BaseCommand):
    help = 'Alert on weixin.'

    def add_arguments(self, parser):
        parser.add_argument('prefix')
        parser.add_argument('--folder', default='scripts')
        parser.add_argument('--redis', default='default')
        parser.add_argument('--script-key', default='SCRIPTS')
        parser.add_argument('--seperator', default=':')


    def handle(self, prefix, folder, redis, script_key, seperator, *args, **options):
        redis = get_redis_connection(redis)
        script_key = seperator.join([prefix, script_key])

        for filename in list_dir(folder):
            if filename.endswith('.lua'):
                fullname = join_path(folder, filename)
                key = filename[:-4]
                print (fullname, end=' --> ')
                script = open(fullname).read()
                sha = redis.script_load(script)
                redis.hset(script_key, key, sha)
                print (sha)

