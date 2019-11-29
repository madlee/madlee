from traceback import format_exc
from django.http import JsonResponse, Http404, HttpResponseRedirect, HttpResponse
from django.conf import settings
from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.views.decorators.csrf import csrf_exempt, ensure_csrf_cookie

from .file import split_path
from .io import load_json




def render_template(request, **argv):
    path = request.get_full_path()[1:]
    return render(request, path, argv)


def json_request(func):
    def new_func(request, *args, **kwargs):
        data = load_json(request.body)
        data.update(kwargs)
        return func(request, *args, **data)
    return new_func


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





try:
    from io import BytesIO
    
    from django.contrib.auth.models import User
    from otpauth import OtpAuth
    import qrcode

    def otp_secret_and_uri(user, site_name, issuer=None):
        salt = user.password.split('$')[2]
        auth = OtpAuth(salt)
        if issuer == None:
            issuer = site_name
        uri = auth.to_uri('totp', '%s@%s' % (user.username, site_name), issuer)
        return salt, uri

    @login_required
    def otp_bar_code(request, user=None, issuer=None):
        if user == None:
            user = request.user
        site_name = request.META['HTTP_HOST']
        secret, uri = otp_secret_and_uri(user, site_name, issuer)
        img = qrcode.make(uri)
        _, filename = split_path(request.META['PATH_INFO'])
        filetype = filename.rsplit('.', maxsplit=1)[-1].upper()
        f = BytesIO()
        img.save(f, filetype)
        result = f.getvalue()
        return HttpResponse(result, secret)

    @login_required
    def otp_totp(request):
        salt = request.user.password.split('$')[2]
        auth = OtpAuth(salt)
        return {
            'code': auth.totp()
        }

    def check_otp_code(user, otp_code):
        salt = user.password.split('$')[2]
        auth = OtpAuth(salt)
        return auth.valid_totp(otp_code)

    @csrf_exempt
    @ensure_csrf_cookie
    @json_request
    @json_response
    def otp_login_action(request, username, password, otp_code):
        try:
            user = User.objects.get(username=username)
        except User.DoesNotExist:
            try:
                user = User.objects.get(email=username)
            except User.DoesNotExist:
                raise RuntimeError('Invalid User')
        
        if not check_otp_code(user, otp_code):
            raise RuntimeError('Invalid 2FA Code')

        user = authenticate(username=username, password=password)
        if user is None:
            raise RuntimeError('Invalid Password')

        login(request, user)


except ImportError:
    pass




def logout_action(request):
    logout(request)
    return redirect('login.html')

