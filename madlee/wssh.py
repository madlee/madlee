#!/bin/bash 
import asyncio as aio
from datetime import datetime as DateTime
from redis import StrictRedis as Redis
from asyncio.subprocess import create_subprocess_shell, PIPE
from lzma import compress, decompress 
from json import loads as load_json, dumps as dump_json
from uuid import uuid4
from hashlib import md5 as MD5
from base64 import b64encode, b64decode
import websockets

from .misc.netware import hostname

BUFFER_SIZE = 16*1024
SECRET_KEY = {}
TIMESTAMP_TOLERANCE = 10

async def run_shell(id, cmd):
    proc = await create_subprocess_shell(
        cmd,
        stdout=PIPE,
        stderr=PIPE
    )
    stdout, stderr = await proc.communicate()
    if stdout:
        stdout = stdout.decode().split('\n')
    else:
        stdout = None
    if stderr:
        stderr = stderr.decode().split('\n')
    else:
        stderr = None

    return {
        'id': id,
        'cmd': cmd,
        'rtn': proc.returncode,
        'stdout': stdout,
        'stderr': stderr
    }

async def redis_commond(websocket, id, cmd, call, **args):
    result = {
        'id': id,
        'cmd': cmd,
        'call': call,
    }
    if call == 'open':
        url = args['url']
        websocket.redis = Redis.from_url(args['url'])
        result['url'] = url
    elif call == 'close':
        del websocket.redis
        websocket.redis = None
        result['redis'] = 'Closed'
    elif call == 'rpush':
        key = args['key']
        data = args['data']
        if type(data) == str:
            data = [data]
        encode = args['encode']
        if encode == 'raw':
            pass
        elif encode == 'base64':
            data = [
                b64decode(row)
                for row in data
            ]
        else:
            assert False
        result['result'] = websocket.redis.rpush(key, *data)
    elif call == 'publish':
        key = args['key']
        data = args['data']
        encode = args['encode']
        if encode == 'raw':
            pass
        elif encode == 'base64':
            data = b64decode(data)
        else:
            assert False
        result['result'] = websocket.redis.publish(key, data)

    return result



async def dump_file(file, md5, content, encode):
    n = len(content)
    if encode == 'xz':
        content = decompress(content)
    file.write(content)
    md5.update(content)
    return {
        'length': n, 'raw_size': len(content)
    }

async def ping(id):
    return {
        'id': id,
        'pong': id
    }


async def pull_file(self, cmd_id, filename, encode):
    print ('####', cmd_id, filename, encode)
    md5 = MD5()
    file = open(filename, 'rb')
    length = raw_size = 0
    while True:
        content = file.read(BUFFER_SIZE)
        if content:
            md5.update(content)
            raw_size += len(content)
            if encode == 'xz':
                content = compress(content)
            length += len(content)
            await self.send(b'\0'+content)
        else:
            break
    return {
        'id': cmd_id, 'encode': encode, 'filename': filename, 
        'raw_size': raw_size, 'length': length, 'md5': md5.hexdigest()
    }


async def login(websocket):
    async for msg in websocket:
        msg = load_json(msg)
        if msg['cmd'] != 'login':
            await websocket.send(dump_json({
                'status': 'NOT LOGIN',
                'message': 'Please Login First.'
            }))
            continue
       
        timestamp = DateTime.fromisoformat(msg['timestamp'])
        if abs((timestamp - DateTime.now()).total_seconds()) > TIMESTAMP_TOLERANCE:
            await websocket.send(dump_json({
                'status': 'EXPIRED',
                'message': 'TIMESTAMP EXPIRED'
            }))
            break
            
        username = msg['username']
        try:
            secret_key = SECRET_KEY[username]
        except KeyError:
            await websocket.send(dump_json({
                'status': 'INVALID USER',
                'message': 'INVALID USER'
            }))
            break

        signature = ':'.join([msg['id'], msg['cmd'], username, msg['timestamp'], secret_key])
        signature = 'MD5:' + MD5(signature.encode()).hexdigest()
        if signature != msg['signature']:
            await websocket.send(dump_json({
                'status': 'INVALID SIGNATURE',
                'message': 'INVALID SIGNATURE'
            }))
            break

        await websocket.send(dump_json({
            'id': msg['id'], 'cmd': 'Login',
            'username': username,
            'status': 'OK',
            'message': 'Login Success'
        }))
        return True

    await websocket.close()
    return False


async def wssh(websocket):
    # Web Socket Shell
    success = await login(websocket)
    if not success:
        return

    async for msg in websocket:
        try:
            if type(msg) == bytes and msg[0] == 0:
                result = await dump_file(websocket.file, websocket.md5, msg[1:], websocket.encode)
            else:
                assert type(msg) == str
                msg = load_json(msg)
                id, cmd = msg['id'], msg['cmd']
                if cmd == 'ping':
                    result = await ping(id)
                elif cmd == 'pull':
                    result = await pull_file(websocket, id, msg['filename'], msg['encode'])
                elif cmd == 'open':
                    websocket.target = target = msg['target']
                    websocket.file = open(target, 'wb')
                    websocket.encode = msg['encode']
                    websocket.md5 = MD5()
                    result = {
                        'id': id, 'cmd': cmd,
                        'target': target
                    }
                elif cmd == 'close':
                    websocket.file.close()
                    websocket.file = None
                    websocket.encode = ''
                    result = {
                        'id': id, 'cmd': cmd,
                        'target': websocket.target,
                        'md5': websocket.md5.hexdigest()
                    }
                    websocket.target = ''
                    websocket.md5 = None
                elif cmd == 'redis':
                    result = await redis_commond(websocket, **msg)
                else:
                    result = await run_shell(id, cmd)
        except Exception as e:
            result = {'ERROR': str(e)}
        result['server_time'] = DateTime.now().isoformat()
        result['host_name'] = HOST_NAME
        print (result)
        await websocket.send(dump_json(result))
        

async def server(host, port):
    async with websockets.serve(wssh, host, port):
        await aio.Future()  # run forever


class AsyncClient:
    def __init__(self, url):
        self.url = url
        self.client = None

    async def connect(self, username, secret_key):
        self.client = await websockets.connect(self.url)
        if username:
            msg = {
                'id': str(uuid4()), 'cmd': 'login',
                'username': username,
                'timestamp': DateTime.now().isoformat()
            }

            signature = ':'.join([msg['id'], msg['cmd'], username, msg['timestamp'], secret_key])
            msg['signature'] = 'MD5:' + MD5(signature.encode()).hexdigest()
            await self.client.send(dump_json(msg))
            response = load_json(await self.client.recv())
            if response['status'] != 'OK':
                raise RuntimeError(response.get('message', 'Login Failed'))
        return self.client


    async def close(self):
        await self.client.close()


    async def run_shell(self, cmd, sync=True):
        id = str(uuid4())
        await self.client.send(dump_json({
            'id': id,
            'cmd': cmd
        }))
        if sync:
            msg = await self.client.recv()
            return load_json(msg), id
        else:
            return id

    async def send_file(self, target, filename, xz=True, sync=True):
        file = open(filename, 'rb')
        id = str(uuid4())
        if xz:
            encode = 'xz'
        else:
            encode = 'raw'

        await self.client.send(dump_json({
            'id': id,
            'cmd': 'open',
            'encode': encode,
            'target': target
        }))

        raw_size = length = 0

        md5 = MD5()
        while True:
            content = file.read(BUFFER_SIZE)
            if content:
                md5.update(content)
                raw_size += len(content)
                if xz:
                    content = compress(content)
                    encode = 'xz'
                else:
                    encode = 'raw'
                length += len(content)
                await self.client.send(
                    b'\0'+ content
                )
            else:
                break
        
        md5 = md5.hexdigest()
        await self.client.send(dump_json({
            'id': id, 'cmd': 'close',
            'raw_size': raw_size, 'length': length
        }))

        if sync:
            async for msg in self.client:
                msg = load_json(msg)
                if 'cmd' in msg and msg['cmd'] == 'close':
                    if md5 == msg['md5']:
                        break
                    else:
                        raise RuntimeError('Transfer File %s --> %s failed.' % (filename, target))
            return msg, id
        else:
            return id

    async def redis(self, url):
        id = str(uuid4())
        msg = {
            'id': id, 'cmd': 'redis',
            'call': 'open',
            'url': url
        }
        await self.client.send(dump_json(msg))
        msg = await self.client.recv()
        self.has_redis = True
        return load_json(msg), id

    async def rpush(self, key, *data):
        assert self.has_redis
        assert len(data)>0
        if type(data[0]) == bytes:
            data = [
                b64encode(row).decode()
                for row in data
            ]
            encode='base64'
        else:
            encode='raw'

        id = str(uuid4())
        msg = {
            'id': id, 'cmd': 'redis',
            'call': 'rpush', 'key': key,
            'encode': encode,
            'data': data
        }
        await self.client.send(dump_json(msg))
        msg = await self.client.recv()
        return load_json(msg), id


    async def publish(self, key, data):
        assert self.has_redis
        if type(data) == bytes:
            data = b64encode(data)
            encode='base64'
        else:
            encode='raw'
        
        id = str(uuid4())
        msg = {
            'id': id, 'cmd': 'redis',
            'call': 'publish', 'key': key,
            'encode': encode,
            'data': data
        }
        await self.client.send(dump_json(msg))
        msg = await self.client.recv()
        return load_json(msg), id



    async def pull_file(self, filename, target, xz=True, sync=True):
        id = str(uuid4())
        msg = {
            'id': id, 'cmd': 'pull',
            'encode': 'xz' if xz else 'raw',
            'filename': filename
        }
        print (msg)
        await self.client.send(dump_json(msg))

        if sync:
            with open(target, 'wb') as output:
                md5 = MD5()
                async for msg in self.client:
                    if type(msg) == bytes:
                        content = msg[1:]
                        if xz:
                            content = decompress(content)
                        md5.update(content)
                        output.write(content)
                    else:
                        break
                msg = load_json(msg)
                print ('####', md5.hexdigest())
                return msg, id
        else:
            return id


    @staticmethod
    async def batch(url, username, secret, cmds):
        client = AsyncClient(url)
        await client.connect(username, secret)
        i = 0
        while i < len(cmds):
            if cmds[i] == 'pull':
                src = cmds[i+1]
                tgt = cmds[i+2]
                result = await client.pull_file(src, tgt, False)
                i += 3
            elif cmds[i] == 'send':
                src = cmds[i+1]
                tgt = cmds[i+2]
                result = await client.send_file(tgt, src, False)
                i += 3
            else:
                result = await client.run_shell(cmds[i])
                i += 1
            print (result)

        await client.close()




class SyncClient:
    def __init__(self, url):
        from websocket import WebSocket
        self.url = url
        self.client = WebSocket()

    def connect(self, username, secret_key):
        self.client.connect(self.url)
        if username:
            msg = {
                'id': str(uuid4()), 'cmd': 'login',
                'username': username,
                'timestamp': DateTime.now().isoformat()
            }

            signature = ':'.join([msg['id'], msg['cmd'], username, msg['timestamp'], secret_key])
            msg['signature'] = 'MD5:' + MD5(signature.encode()).hexdigest()
            self.client.send(dump_json(msg))
            response = load_json(self.client.recv())
            if response['status'] != 'OK':
                raise RuntimeError(response.get('message', 'Login Failed'))
        return self.client


    def close(self):
        self.client.close()


    def run_shell(self, cmd):
        id = str(uuid4())
        self.client.send(dump_json({
            'id': id,
            'cmd': cmd
        }))
        msg = self.client.recv()
        return load_json(msg), id

    def send_file(self, target, filename, xz=True):
        file = open(filename, 'rb')
        id = str(uuid4())
        if xz:
            encode = 'xz'
        else:
            encode = 'raw'

        self.client.send(dump_json({
            'id': id,
            'cmd': 'open',
            'encode': encode,
            'target': target
        }))

        raw_size = length = 0

        md5 = MD5()
        while True:
            content = file.read(BUFFER_SIZE)
            if content:
                md5.update(content)
                raw_size += len(content)
                if xz:
                    content = compress(content)
                    encode = 'xz'
                else:
                    encode = 'raw'
                length += len(content)
                self.client.send(
                    b'\0'+ content
                )
            else:
                break
        
        md5 = md5.hexdigest()
        self.client.send(dump_json({
            'id': id, 'cmd': 'close',
            'raw_size': raw_size, 'length': length
        }))

        for msg in self.client:
            msg = load_json(msg)
            if 'cmd' in msg and msg['cmd'] == 'close':
                if md5 == msg['md5']:
                    break
                else:
                    raise RuntimeError('Transfer File %s --> %s failed.' % (filename, target))
        return msg, id
        

    def redis(self, url):
        id = str(uuid4())
        msg = {
            'id': id, 'cmd': 'redis',
            'call': 'open',
            'url': url
        }
        self.client.send(dump_json(msg))
        msg = self.client.recv()
        self.has_redis = True
        return load_json(msg), id

    def rpush(self, key, *data):
        assert self.has_redis
        assert len(data)>0
        if type(data[0]) == bytes:
            data = [
                b64encode(row).decode()
                for row in data
            ]
            encode='base64'
        else:
            encode='raw'

        id = str(uuid4())
        msg = {
            'id': id, 'cmd': 'redis',
            'call': 'rpush', 'key': key,
            'encode': encode,
            'data': data
        }
        self.client.send(dump_json(msg))
        msg = self.client.recv()
        return load_json(msg)


    def publish(self, key, data):
        assert self.has_redis
        if type(data) == bytes:
            data = b64encode(data)
            encode='base64'
        else:
            encode='raw'
        
        id = str(uuid4())
        msg = {
            'id': id, 'cmd': 'redis',
            'call': 'publish', 'key': key,
            'encode': encode,
            'data': data
        }
        self.client.send(dump_json(msg))
        msg = self.client.recv()
        return load_json(msg), id

    def pull_file(self, filename, target, xz=True):
        id = str(uuid4())
        msg = {
            'id': id, 'cmd': 'pull',
            'encode': 'xz' if xz else 'raw',
            'filename': filename
        }
        print (msg)
        self.client.send(dump_json(msg))

        with open(target, 'wb') as output:
            md5 = MD5()
            for msg in self.client:
                if type(msg) == bytes:
                    content = msg[1:]
                    if xz:
                        content = decompress(content)
                    md5.update(content)
                    output.write(content)
                else:
                    break
            msg = load_json(msg)
            print ('####', md5.hexdigest())
            return msg, id


    @staticmethod
    def batch(url, username, secret, cmds):
        client = SyncClient(url)
        client.connect(username, secret)
        i = 0
        while i < len(cmds):
            if cmds[i] == 'pull':
                src = cmds[i+1]
                tgt = cmds[i+2]
                result = client.pull_file(src, tgt, False)
                i += 3
            elif cmds[i] == 'send':
                src = cmds[i+1]
                tgt = cmds[i+2]
                result = client.send_file(tgt, src, False)
                i += 3
            else:
                result = client.run_shell(cmds[i])
                i += 1
            print (result)

        client.close()




if __name__ == '__main__':
    import sys
    host, port, config = sys.argv[1:]
    config = load_json(open(config).read())
    TIMESTAMP_TOLERANCE = int(config.get('tolerance', TIMESTAMP_TOLERANCE))
    SECRET_KEY = config['USERS']
    HOST_NAME = hostname()
    aio.run(server(host, int(port)))


