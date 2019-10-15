''' Qiye Weixin '''

WEIXIN_TOKEN_URL    = 'https://qyapi.weixin.qq.com/cgi-bin/gettoken'
WEIXIN_QUESTES_URL  = "https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token="


from datetime import datetime as DateTime, timedelta as TimeDelta
from json import loads as load_json, dumps as dump_json
import logging
import requests




class QiyeWeixin:
    def __init__(self, corp_id, secret, agentid):
        self.corp_id    = corp_id
        self.secret     = secret
        self.agentid    = agentid
        self.__message  = []

        self.__access_token = None


    def get_token(self):
        if self.__access_token is None or self.__access_token[0] < DateTime.now():

            values = {
                'corpid': self.corp_id,
                'corpsecret': self.secret
            }
            req = requests.post(WEIXIN_TOKEN_URL, params=values)
            data = load_json(req.text)
            access_token = data["access_token"]
            next_tick = DateTime.now() + TimeDelta(seconds=data['expires_in'])
            self.__access_token = next_tick, access_token

        return self.__access_token[1]


    def push(self, msg):
        self.__message.append(msg)


    def reset(self):
        self.__message = []


    def commit(self, touser='@all', toparty='', totag='', join='\n'):
        message = join.join(self.__message).strip()
        if message:
            self.__message = []
            return self.send_text(
                message, touser, toparty, totag
            )


    def send_text(self, message, touser='@all', toparty='', totag=''):
        values = {
            "touser":   touser,
            "toparty":  toparty,
            "totag":    totag,
            "msgtype":  "text",
            "agentid":  self.agentid,
            "text":     {
                "content": message.strip()
            },
            "safe":     "0"
        }

        url = WEIXIN_QUESTES_URL + self.get_token()
        result = requests.post(url, dump_json(values, ensure_ascii=False).encode('utf-8')).json()
        return result




class WeixinHandle(logging.Handler):
    terminator = '\n'

    def __init__(self, corp_id, secret, agentid, auto_commit=False):
        super().__init__()
        self.__weixin = QiyeWeixin(corp_id, secret, agentid)
        self.__auto_commit = auto_commit


    def emit(self, record):
        try:
            msg = self.format(record)
            self.__weixin.push(msg)
            if self.__auto_commit:
                self.commit(touser='@all')

        except Exception:
            self.handleError(record)


    def push(self, msg):
        self.__weixin.push(msg)


    def reset(self):
        self.__weixin.reset()


    def send_text(self, message, touser='@all', toparty='', totag=''):
        return self.__weixin.send_text(message, touser, toparty, totag)


    def commit(self, touser='@all', toparty='', totag=''):
        self.__weixin.commit(touser=touser, toparty=toparty, totag=totag, join=self.terminator)


def get_weixin(logger):
    for h in logger.handlers:
        try:
            if isinstance(h, WeixinHandle):
                return h
        except NameError:
            pass        
    return None

