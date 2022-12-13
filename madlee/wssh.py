#!/bin/bash 
import asyncio as aio
from datetime import datetime as DateTime
from asyncio.subprocess import create_subprocess_shell, PIPE
from lzma import compress, decompress 
from json import loads as load_json, dumps as dump_json
from uuid import uuid4
import websockets

from .misc.netware import hostname

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


async def dump_file(msg):
    cmd, content = msg.split(b'\0', 1)
    cmd = load_json(cmd)
    raw_length =len(content)
    id = cmd['id']
    encode = cmd['encode']
    target = cmd['target']
    if encode == 'xz':
        content = decompress(content)
    open(target, 'wb').write(content)
    return {
        'id': id, 'encode': encode, 'target': target, 
        'size': len(content), 'raw_length': raw_length
    }

async def ping(id):
    return {
        'id': id,
        'pong': id
    }


async def pull_file(self, cmd_id, filename, encode):
    print ('####', cmd_id, filename, encode)
    content = open(filename, 'rb').read()
    raw_length = len(content)
    if encode == 'xz':
        content = compress(content)
    print ('####', raw_length, len(content))
    await self.send(b'\0'+content)
    return {
        'id': cmd_id, 'encode': encode, 'filename': filename, 
        'size': len(content), 'raw_length': raw_length
    }




async def wssh(websocket):
    # Web Socket Shell
    async for msg in websocket:
        try:
            if type(msg) == bytes and msg[0] == 0:
                result = await dump_file(msg[1:])
            else:
                print (msg)
                assert type(msg) == str
                msg = load_json(msg)
                id, cmd = msg['id'], msg['cmd']
                print (id, cmd)
                if cmd == 'ping':
                    result = await ping(id)
                elif cmd == 'pull':
                    result = await pull_file(websocket, id, msg['filename'], msg['encode'])
                else:
                    result = await run_shell(id, cmd)
        except Exception as e:
            result = {'ERROR': str(e)}
        result['server_time'] = DateTime.now().isoformat()
        result['host_name'] = HOST_NAME
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

    async def send_file(self, target, file, xz=True, sync=True):
        try:
            content = file.read()
        except AttributeError:
            content = open(file, 'rb').read()
            
        if xz:
            content = compress(content)
            encode = 'xz'
        else:
            encode = 'raw'

        id = str(uuid4())
        msg = {
            'id': id,
            'encode': encode,
            'target': target
        }
        await self.client.send(
            b'\0'+ dump_json(msg).encode() +b'\0'+content
        )
        if sync:
            msg = await self.client.recv()
            return load_json(msg), id
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
            msg = await self.client.recv()
            if type(msg) == bytes:
                content = msg[1:]
                if xz:
                    content = decompress(content)
                open(target, 'wb').write(content)
                msg = await self.client.recv()
            return load_json(msg), id
        else:
            return id



        





if __name__ == '__main__':
    import sys
    host, port = sys.argv[1:]
    HOST_NAME = hostname()
    aio.run(server(host, int(port)))