from traceback import format_exc
from django.http import JsonResponse, Http404, HttpResponseRedirect
from django.conf import settings




def json_response(func):
    try:
        server_info = settings.SERVER_INFO
    except AttributeError:
        server_info = None
    def new_func(request, *args, **kwargs):
        code = 200
        try:
            data = func(request, *args, **kwargs)
            result = {
                'status': 'OK',
                'data': data
            }
        except KeyError as e:
            code = 500
            result = {
                'status':  'error',
                'message': 'Missing Parameter %s' % str(e),
                'type': str(type(e)),
            }
        except Exception as e:
            code = 500
            result = {
                'status': 'error',
                'message': str(e),
                'type': str(type(e)),
            }
            if settings.DEBUG:
                result['debug'] = format_exc()

        if server_info:
            result['server'] = server_info
        response = JsonResponse(result, status=code)
        response['Access-Control-Allow-Origin'] = '*'
        return response
            
    return new_func




def run_forever(seconds, msg_template='%%s', exc_info=True):
    def decorator(func):
        def new_func(logger, *args, **kwargs):
            while True:
                try:
                    return func(logger, *args, **kwargs)
                except Exception as e:
                    logger.error(msg_template % (kwargs), e, exc_info=exc_info)
                    from time import sleep
                    sleep(seconds)
        return new_func
    return decorator



def async_run_forever(seconds, msg_template='%%s', exc_info=True):
    def decorator(func):
        async def new_func(logger, *args, **kwargs):
            while True:
                try:
                    return await func(logger, *args, **kwargs)
                except Exception as e:
                    print (msg_template)
                    print (kwargs)
                    logger.error(msg_template % (kwargs), e, exc_info=exc_info)
                    from asyncio import sleep
                    await sleep(seconds)
        return new_func
    return decorator



