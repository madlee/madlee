from base64 import b64encode
from traceback import format_exc
from django.http import JsonResponse, Http404, HttpResponseRedirect, HttpResponse
from django.http import HttpResponseForbidden, HttpResponseNotFound
from django.conf import settings
from django.db import IntegrityError, transaction
from django.shortcuts import render, redirect
from django.forms import model_to_dict
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.models import User
from django.contrib.auth.decorators import login_required
from django.views.decorators.csrf import csrf_exempt, ensure_csrf_cookie

from .file import split_path
from .io import load_json


class ResponseJsonError(RuntimeError):
    def __init__(self, message, data, code=200):
        self.__message = message
        self.__data = data
        self.__code = code

    @property
    def result(self):
        return {
            'status': 'error',
            'message': self.__message,
            'data': self.__data,
            'type': str(type(self))
        }

    @property
    def code(self):
        return self.__code


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
            if isinstance(data, HttpResponse):
                return data
            result = {
                'status': 'OK',
                'data': data
            }
        except ResponseJsonError as e:
            code = e.code
            result = e.result
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
    def otp_bar_code(request, issuer=None):
        username = request.GET.get('username', None)
        if username == None or username == request.user.username:
            user = request.user
        else:
            if request.user.is_superuser:
                try:
                    user = User.objects.get(username=username)
                except User.DoesNotExist:
                    return HttpResponseNotFound()
            else:
                return HttpResponseForbidden()

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
        user = authenticate(username=username, password=password)
        if user is None:
            raise ResponseJsonError(
                'Invalid Username/Password',
                {'password': 'Invalid', 'username': 'Invalid'}
            )

        if not check_otp_code(user, otp_code):
            raise ResponseJsonError(
                'Invalid 2FA Code',
                {'otp_code': 'Invalid'}
            )
        login(request, user)


    @csrf_exempt
    @ensure_csrf_cookie
    @json_request
    @json_response
    def register_action(request, username, password, active=False, site_name=None, issuer=None, filetype='png'):
        try:
            user = User.objects.create_user(username=username, email=username, password=password, is_active=False)
        except IntegrityError:
            raise ResponseJsonError(
                'User "%s" has existed.' % username,
                {'username': 'Existed'}
            )
        
        if not site_name:
            site_name = request.META['HTTP_HOST']
        if issuer == None:
            issuer = site_name

        secret, uri = otp_secret_and_uri(user, site_name, issuer)
        img = qrcode.make(uri)
        f = BytesIO()
        img.save(f, filetype)
        code = b64encode(f.getvalue()).decode()

        request.session['totp_secret'] = secret

        return {
            'username': username,
            'secret': secret,
            'qrcode': code
        }


    @json_response
    def check_2fa_code(request):
        code = request.GET['code_2fa']
        totp_secret = request.session['totp_secret']
        auth = OtpAuth(totp_secret)
        if auth.valid_totp(code):
            del request.session['totp_secret']
            return {
                'success': "OK"
            }
        else:
            raise ResponseJsonError(
                "Invalid 2FA Code",
                {'code_2fa': 'Invalid'}
            )

except ImportError:
    pass




@csrf_exempt
@ensure_csrf_cookie
@json_request
@json_response
def login_action(request, username, password):
    user = authenticate(username=username, password=password)
    if user is None:
        raise ResponseJsonError(
            'Invalid Username/Password',
            {'password': 'Invalid', 'username': 'Invalid'}
        )
    login(request, user)




def logout_action(request):
    logout(request)
    return redirect('login.html')



