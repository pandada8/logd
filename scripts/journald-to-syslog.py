#!/bin/python 

"""
this scripts pull log from journald then reassemble to syslog format and send to the udp port

usage: $1 [[host] port]
example:
$1 localhost
$1 localhost 1514
"""
import socket
import sys
import json
import os
import syslog
import datetime

PORT=1514
HOST="localhost"
FILE="./log.json"

GROUP=["SYSLOG_FACILITY", "SYSLOG_PID", "PRIORITY", "SYSLOG_IDENTIFIER", "MESSAGE"]

def get_id(cursor):
    payload = dict([i.split('=') for i in cursor.split(';')])
    return payload['x']

def get_struct(obj):
    struct = {i: obj[i] for i in obj if i[0] != '_' and i not in GROUP}
    if len(struct) == 0:
        return ' -'
    ret = " [test {}]".format(" ".join(i + '=' + json.dumps(j) for i, j in struct.items() if '\n' not in j))
    return ret

def send_log(obj):
    try:
        pri = int(obj.get('SYSLOG_FACILITY', obj.get("PRIORITY", 0))) + 8 * 10
        ts = datetime.datetime.fromtimestamp(int(obj['__REALTIME_TIMESTAMP']) / 1e6).isoformat() + 'Z'
        hostname = obj['_HOSTNAME']
        appname = obj.get('_EXE', '???')
        procid = obj['_PID']
        msgid = get_id(obj['__CURSOR'])
        
        data = "<{}>1 {} {} {} {} {}".format(pri, ts, hostname, appname, procid, msgid)
        data += get_struct(obj)
        data += ' ' + obj['MESSAGE']
        s=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.sendto(data.encode("utf8"), (HOST, PORT))
    except:
        print(obj)
    # print(data)


def getLogs():
    logs = []
    if os.path.exists(FILE):
        with open(FILE) as fp:
            for num, line in enumerate(fp):
                try:
                    yield json.loads(line)
                except ValueError:
                    print("bad line {}".format(num))
    else:
        # load data from journal daemon directly
        pass

def genSyslog():
    for log in getLogs():
        send_log(log)

def main():
    global s
    if len(sys.argv) == 4:
        PORT = int(sys.argv[3])
    if len(sys.argv) == 3:
        HOST = sys.argv[2]
    
    genSyslog()

if __name__ == "__main__":
    main()