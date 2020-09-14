import socket
import os
import sys
import time
from _thread import *
import ClientObj as co
import dbfuns as db
import pandas as pd
import json
from json.decoder import JSONDecodeError
import ast
from collections import namedtuple

ThreadCount = 0
ThreadClosed = 0
address = ''
maxconn = 50
thdict ={}                           # keep track of all the threads

def pthdict():
    s = ''
    for v  in thdict.values():
        stime = co.timefmt(v.stime)
        line = f'thread {v.tid} {v.raddr} active={v.active} started={stime} '
        s += line
    return line

def act2table(act):
    tab = 'Flut' + act
    return tab

def log_message(rhost, userid, msg, tab='Flutlog'):
    t = time.time()
    stime = time.strftime("%D %R", time.localtime(t))
    stmt = f"insert into {tab} values ('{rhost}', '{stime}', '{userid}', '{msg}')"
    db.cur.execute(stmt)
    db.conn.commit()

def login_check_passwd(obj, usertab='Fusers'):          # check password and return myid
    myid = 0
    uid = obj.userid
    lst = db.dexec(f'select * from {usertab} where userid ="{uid}" ')
    if len(lst) == 0:
        lcnt = db.dexec(f'select count(*) from {usertab}')
        if lcnt[0] == 0:
            myid = 5000
        else:
            lcnt = db.dexec(f'select max(id) from {usertab}')
            myid =  lcnt[0] + 1        # first free id
    
    rows = db.dexec(f'select * from {usertab} where userid="{uid}" ', True) 
    if len(rows) > 0:                       #userid in Table
        dbpasswd = rows[0][5]
        if dbpasswd == obj.passwd: 
            myid = rows[0][4]
            print(f'{obj.userid} has myid={myid}')
        else:
            print(f'{obj.userid} passwd mismatch')
            myid = -1
    else:
        gender = 'Male' if obj.gender.startswith('m') else 'Female'
        stmt = f"insert into {usertab} values ('{uid}', '{obj.fname}', '{obj.lname}', '{gender}', {myid}, '{obj.passwd}')"
        db.cur.execute(stmt)
        db.conn.commit()
    return myid

def db_login(obj, raddr, tab='Fusers'):
    print("dblogin")
    myid = login_check_passwd(obj)
    msg = "bad passwd" if myid == -1 else "logged in"
    log_message(raddr, obj.userid, msg)
    return str(myid)

def data_check_passwd(obj, usertab='Fusers'):          # check password and return myid
    dbid = obj.dbid
    cmd = f'select * from {usertab} where id ="{dbid}"'

    lst = db.dexec(cmd, True)
    return lst

def remove_from_list(lst, words):
    for w in words:
        if w in lst:
            lst.remove(w)

    return w

def check_data_cols(obj, table):
    dbcols = db.headings(table)
    dlen = len(dbcols)
    i = 0
    for c in obj.cols:
        if c in dbcols:
            i += 1
    yn = True if dlen == i + 2 else False
    return yn

def save_data(obj, raddr, lst):
    table = act2table(obj.activity)
    userid = lst[0][0]
    if check_data_cols(obj, table) == False :
        return f"savedata failed, column mismatch for {obj.dbid}"

    for l in obj.data:
        d = dict(zip(obj.cols, l))
        d['activityname'] = obj.activity
        d['userid'] = userid
        db.dict2db(d, table)
    return f"savedata succeeded for {obj.dbid}"

def db_save_data(obj, raddr):
    print("savedata")
    uid = obj.dbid
    myid = -1
    lst = []
    if int(uid) < 0:
        msg = f'invalid uid {uid}'
    else:
        lst = data_check_passwd(obj)
    if len(lst) > 0:
        myid = lst[0][4]
        if myid > 0 and str(myid) == uid:
            msg = save_data(obj, raddr, lst)
    log_message(raddr, uid, msg)
    return msg

def retrieve_cols(table):
    cols = db.headings(table)
    cols.remove('activityname')
    cols.remove('userid')
    res = str(cols)[1:-1]
    res = res.replace("'", "")
    return res

def date2idate(cdate):              # date 1/1/20 to idate 
    idate = 0 
    cpart = cdate.split('/')
    if len(cpart)  == 3:
        month = int(cpart[0])
        day = int(cpart[1])
        yr = int(cpart[2])
        if (yr < 2000):
            yr += 2000
    idate = 10000*yr + 100*month + day
    return idate

def recs2blk(recs, pktrecs):                # total records in list to blks
    _blk = 0
    if recs > pktrecs:
        blks = recs / pktrecs
        _blk = int(blks)
        fract = blks - _blk
        if fract > 0:
            _blk += 1
    return _blk


def readclient(cl):
    try:
        resp = cl.conn.recv(1024*16)                            # wait for client
    except ConnectionResetError as e:
        print(str(e))
        resp = ''
    return resp

########################################################
def splitsendobj(sobj, dlist, pktrecs, cl):
    recs = len(dlist)
    print(f'splitting {recs} records')
    s = 0 
    x = pktrecs
    _blk = recs2blk(recs, pktrecs)
    while _blk > 0:                             #a send all blocks here
        dslice = dlist[s:x]
        sobj = sobj._replace(data=dslice)
        sobj =sobj._replace(blk=_blk)
        sobj =sobj._replace(recs=len(dslice))
        sobj =sobj._replace(slice=[s, x])
        print(f'blk={_blk} recs {s} to {x} ')
        dstr = json.dumps(sobj._asdict())
        send_client(cl, dstr)
        msg = f'sending {sobj.activity} start {sobj.bdate} rows {s} to {x}'
        log_message(cl.raddr, sobj.uid, msg)
        s = x + 1
        if s > recs:
            break
        x += pktrecs
        if x > recs:
            x = recs+1
        _blk -= 1
        print('splitsend: waiting on client')
        try:
            resp = cl.conn.recv(1024*16)                            # wait for client
        except ConnectionResetError as e:
            print(str(e))
            break

        print(f'splitsend received {resp}')
    return ''
##################################################

def db_query(obj, cl):                              # newcode
    table = obj.activity                            # any table 
    userid = db.dbid2userid(obj.dbid)
    bdate = obj.bdate
    if isinstance(bdate, str) and bdate.index('/') > 0:
        bdate = date2idate(bdate)
    cols = retrieve_cols(table)
    recs = obj.maxrecs
    if recs == 0:
        cmd = f'select {cols} from {table} where userid="{userid}" and date > "{bdate}" '
    else:
        cmd = f'select {cols} from {table} where userid="{userid}" and date > "{bdate}" limit {recs} '
    if len(userid) > 0:
        dlist = db.query2list(cmd)
        dstr = json.dumps(dlist)
        # create sendObj to client
        sobj = co.sendObj(obj.action, userid, bdate, table, cols, len(dlist), len(dstr))
        jsonsobj = json.dumps(sobj._asdict())
        send_client(cl, jsonsobj)
        # now save obj for client to retrieve 
        cl.jstr  = dstr
        cl.userid = userid
        cl.table = table
    return '', cmd


def db_send_data(obj, raddr, cl):       # send data to app
    print("senddata")
    uid = obj.dbid
    myid = -1
    lst = []
    if int(uid) > 0:
        lst = data_check_passwd(obj)
    else:
        jstr = 'login failed with dbid=-1'
    if len(lst) > 0:
        myid = lst[0][4]
        userid = lst[0][0]
        if myid > 0 and str(myid) == str(uid):
            jstr, cmd = db_query(obj, cl)
            log_message(raddr, userid, cmd)
        else:
            msg = f'login failed myid={myid} '
            log_message(raddr, uid, msg)
            jstr = msg
    return jstr

def json_standard_load(resp):
    obj = None
    try:
        obj = json.loads(resp)
    except JSONDecodeError as e:
        print(f' json error: {e}')
    except TypeError as e:
        print(f' json type error: {e}')
    return obj

def obj_response(obj, raddr, cl):
    res = obj.action + ' undefined'
    if obj == None:
        return "cant decode object"
    if obj.action == 'login':
        res = db_login(obj, raddr, 'Fusers')
        print(f'jsn response={res}')
        return res
    elif obj.action == 'savedata':
        res = db_save_data(obj, raddr)
    elif obj.action == 'senddata':
        res = db_send_data(obj, raddr, cl)
    return res

def jsn_response(resp, raddr, cl):
    obj = db.jstr2Obj(resp)
    if obj == None:
        obj = json_standard_load(resp)

    res = obj_response(obj, raddr, cl)
    return res

def parse_response(resp, cl, raddr):
    resp = resp.rstrip()
    if resp.startswith('{'):
        reply = jsn_response(resp, raddr, cl)
        #if not reply.endswith('\n'):reply = reply + '\n'
        return reply
        
    if resp.startswith('"'):
        jstr = ast.literal_eval(resp)
        rdict = ast.literal_eval(jstr)
        obj = namedtuple('MyObj', rdict.keys())(**rdict)
        res = obj_response(obj, raddr, cl)
        return res

    lst = resp.split()
    if len(lst) == 0:
        return 'nil'

    if lst[0] == '@login':
        print("new login ")
        cl.userid = lst[1]
        cl.passwd = lst[2]

    elif lst[0] == '@pp':
        return cl.pp()

    elif lst[0] == '@dict':
        return pthdict()

    elif lst[0] == '@data':
        msg = f'@data : sending client {cl.tid} {len(cl.jstr)} chars'
        log_message(raddr, cl.userid, msg)
        return cl.jstr

    elif lst[0] == '@list':
        return "server: @pp @dict @list @data @login"

    elif lst[0] == '@m1':
        return "this is message 1"

    else:
        return resp + ' invalid'


def send_client(cl, reply):                                     # cl object 
    GoodPipe = True
    encoded = str.encode(reply)
    print(f'len reply={len(reply)} len encoded={len(encoded)}')
    try:
        cl.conn.sendall(encoded)
    except BrokenPipeError as e:
        print(str(e))
        GoodPipe = False
    return GoodPipe

def threaded_client(connection):
    global ThreadCount, ThreadClosed, address
    tid = ThreadCount
    raddr = address[0]
    print(f'client myid={tid} raddr={raddr}')
    print('starting threaded client')
    cl = co.ClientObj(connection, tid, raddr)
    #cl.host = raddr
    thdict[tid] = cl
    #connection.send(str.encode(f'Welcome to the Server tid={cl.tid}\n') )
    pipeup = True
    while True:
        try:
            resp = connection.recv(1024*16)
        except ConnectionResetError as e:
            print(str(e))
            break
        if not resp:
            break
        try:
            ans = resp.decode('utf-8')
        except UnicodeDecodeError as e:
            uderr = (str(e))
            print(uderr)
            return uderr
            
        print(f'client=<{ans}>')
        reply = parse_response(ans, cl, raddr)
        if len(reply) > 0:
            pipeup = send_client(cl, reply)

        if not pipeup:
            break 

    ThreadClosed +=1
    print(f'closing client {tid} [{raddr}] closedcnt={ThreadClosed}')
    if ThreadCount == ThreadClosed:
        print('No active clients')
    connection.close()


def start_server(port):
    global ThreadCount, ThreadClosed, address
    serverSocket = socket.socket()
    host = '127.0.0.1'
    host = '192.168.1.40'

    print(f'try to bind port {port}')

    try:
        serverSocket.bind((host, port))
        print(f'started server on {port}, max connections={maxconn}')
    except socket.error as e:
        print(str(e))
        serverSocket = None

    if serverSocket == None:
        print('sockserver exiting')
        return

    serverSocket.listen(maxconn)        #up t0 100 connections at a time

    while True:
        print('Waiting for a Connection..')
        Client, address = serverSocket.accept()
        print('Connected to: ' + address[0] + ':' + str(address[1]))
        print(f'client={Client}')
        start_new_thread(threaded_client, (Client,) )
        ThreadCount += 1
        starttime = time.time
        print(f'Thread Number: {ThreadCount}, startime={starttime}')

    serverSocket.close()

port = 6763

if __name__ == '__main__':
    args = sys.argv[0:]
    if args:
        argc = len(args)
        if argc > 1:
            port = int(args[1])
        if argc >2:
            dir = args[2]

db.conndb()
start_server(port)