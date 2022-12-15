#!/bin/bash 
import asyncio as aio
from datetime import datetime as DateTime
from asyncio.subprocess import create_subprocess_shell, PIPE
from lzma import compress, decompress 
from json import loads as load_json, dumps as dump_json
from uuid import uuid4
from hashlib import md5 as MD5
import websockets

from .misc.netware import hostname

BUFFER_SIZE = 16*1024

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





async def wssh(websocket):
    # Web Socket Shell
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


class Client:
    def __init__(self, url):
        self.url = url
        self.client = None

    async def connect(self):
        self.client = await websockets.connect(self.url)
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




if __name__ == '__main__':
    import sys
    host, port = sys.argv[1:]
    HOST_NAME = hostname()
    aio.run(server(host, int(port)))
