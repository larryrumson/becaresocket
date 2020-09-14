import socket
import sys
import ClientObj as co
import dbfuns as db
import json
import pandas as pd

if not dir().count('idmap'):
    idmap = {}

if not dir().count('passmap'):
    passmap = {}

if not dir().count('client'):
    client = None

def tcp_client(host, port, timeout=180.0):         # timeout is 3 secs
    if timeout > 0:
        socket.setdefaulttimeout(timeout)
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client.connect( (host, port) )
        print(f'try connecting to {host}!{port}')
    except ConnectionRefusedError as e:
        print(e)
    return client

def tcp_send(client, text, doresp=True, timeout=10.0, pktsize=1024*12):    # seconds
    ans = ''
    if not text.endswith('\n'):
        text = text + '\n'
    #socket.setdefaulttimeout(timeout)
    client.sendall(text.encode('utf-8'))
    if doresp:
        resp = client.recv(pktsize)
        ans = resp.decode('utf-8')
    return ans

def read_server():
    global client, pktsize
    resp = client.recv(pktsize)
    ans = resp.decode('utf-8')
    return ans

# exec(open('clientsock.py').read())

host ='50.210.226.172'
host = '192.168.1.40'
port = 6763
pktsize = 1024*32

#####                   globals used for testing, track current login
myid = '-1'
mypass = ''
myuser = ''
myact = 'TwentyFiveSteps'
###############              end globals ###########################

#client = None

def tryconnect(dbuser, passwd):
    print(f'dbuser={dbuser}')
    return db.conndb(host, 3306, dbuser, passwd)

def mkpasswd(str):
    return co.do_hash(str)
 
def doaction(str):
    global client
    s = 'nomatch'
    return s

def toserver(str, doresp=True):
    global client, host, port
    if client == None:
        client = tcp_client(host, port)
    resp = tcp_send(client, str, doresp)
    return resp

def inputloop():
    s = 'h'
    while s != 'q':
        s = input('send> ')
        if s.startswith('$'):
            s = doaction(s)
        resp = tcp_send(client, s)
        print(resp)

def sendobj(nmtup, doresp=True):             # serialize a namedtuple and then send
    global client, host, port
    if client == None:
        client = tcp_client(host, port)

    jstr = json.dumps(nmtup._asdict())
    return toserver(jstr, doresp)

def saveobj(obj, file='/tmp/obj.jsn'):
    jstr = json.dumps(obj._asdict())
    with open(file, 'w+') as f:
        f.write(jstr)
    return jstr

def sockclose():
    global client
    if client != None:
        print("closing client")
        client.close()
        client = None
    else:
        print('socket already closed')

def sendlogin(obj):
    global myid, mypass, myuser
    if not isinstance(obj, co.loginObj):
        print("obj is not co.loginObj")
    else:
        myid = sendobj(obj)
        if myid == -1:
            print(f'{obj.userid} : invalid userid or password')
        else:
            idmap[obj.userid] = myid      
            passmap[obj.userid] = obj.passwd
            mypass = obj.passwd
            myuser = obj.userid
    return myid

def gen_testing_data(act, userid, recs=0):
    print(f'gentesting recs={recs}')
    df = db.sel(userid, act, recs)
    rf = db.prepare_df(df)
    dlst = rf.values.tolist()
    rcols = list(rf)
    #jstr = pf.to_json()
    # xf = pd.read_json(jstr)   # will read json to df
    return dlst, rcols

def senddata(act, lst, cols):            # lst from gen_testing_data()
    resp = ''
    #robj = co.rqstObj('savedata', myid, mypass, act)
    recs = len(lst)
    dobj = co.dataObj('savedata', myid, mypass, act, lst, recs)
    resp = sendobj(dobj)
    return resp

def test_df_obj(act, userid, recs=0):
    dlst, dcols = gen_testing_data(act, userid, recs)   
    dobj = co.dataObj('savedata', myid, mypass, act, dlst, dcols, len(dlst))
    return dobj, dcols
    #return send_data(act, jdata)


def send_and_show_data(myact, userid, recs):
    dobj, cols = test_df_obj(myact, userid, recs)
    print(dobj)
    print('use sendobj to encode as json and send to server')
    sendobj(dobj)

    print('see that our data was inserted')
    tab = 'Flut' + myact
    print(f'using table {tab}')
    cmd = f'select * from {tab}'
    cols = db.headings(tab)
    df = db.query2df(cmd, cols)
    print(df)

def decode(jstr):
    obj = None
    try:
        obj = json.loads(jstr)
    except json.JSONDecodeError as e:
        print(str(e))
        print(f'decode:jstr={jstr}')
    return obj

def get_server_data(obj):
    totbytes = obj['strlen']
    jstr = toserver('@data', True)
    while len(jstr) < totbytes:
        #resp = toserver('@data', True)
        resp = read_server()
        jstr += resp
        print(f'jstr bytes={len(jstr)} len(resp)={len(resp)}')
    
    print(f'rcved totbytes={totbytes}')
    dlist = decode(jstr)
    return dlist

def req_saved_data(sobj):
    res = sendobj(sobj)                 # descriptor object
    obj = decode(res)
    cols = obj['cols'].split(',')
    dlist = get_server_data(obj)
    df = pd.DataFrame(dlist, columns=cols)
    return df

if __name__ == '__main__':
    args = sys.argv[0:]
    if args:
        argc = len(args)
        if argc > 2:
            host = args[1]
            port = int(args[2])
        elif argc > 1:
            port = int(args[1])
    if client == None:
        client = tcp_client(host, port)

    doloop = False
    if doloop:
        inputloop()

    #db.dbid2userid('5003')

    ## test loginObj
    l3 = co.loginObj('login', 'lpat3', 'ed2ee141cb338935e246b60c94f01a72', 'larry3', 'rubin3', 'm')
    l4 = co.loginObj('login', 'lpat4', 'f42087059b37ae7f4d9f0d3a475801a8', 'larry4', 'rubin4', 'm')
    l5 = co.loginObj('login', 'lpat5',  f'{mkpasswd("cms")}', 'larry5', 'rubin5', 'm') 
    l6 = co.loginObj('login', 'lpat6',  f'{mkpasswd("cms1")}', '', '', '') 

    #l3 = db.rfile('/tmp/lpat3')
    #l4 = db.rfile('/tmp/lpat4')
    print('ids for testing: l1 l3 l4 l5')

    l1 = co.loginObj('login', 'lpat1', f'{mkpasswd("lpat1")}', 'larry1', 'rubin1', 'm')

    yn = 'n'
    if yn.startswith('y'):
        myid = sendlogin(l1)           # try logging in
        if int(myid) > 0:
            print(f'logged in as {myuser} id={myid} pass={mypass}')

            print('call tryconnect with your mysql login and passwd')
            myconn = tryconnect('lrubin', 'vision11')

    yn = 'no'
    myact = 'Path2'
    userid = 'lpat1'
    #yn = input('send over some test data? ')
    if yn.startswith('y'):
        recs = 10 
        dobj, cols = test_df_obj(myact,userid ,recs)
        print(dobj)
        print('use sendobj to encode as json and send to server')
        sendobj(dobj)

        print('see that our data was inserted')
        tab = 'Flut' + myact
        print(f'using table {tab}')
        cmd = f'select * from {tab} where userid="{userid}" '
        cols = db.headings(tab)
        df = db.query2df(cmd, cols)
        print(df)

    act = 'Path2'
    myid = 5003
    s1 = co.savedObj('senddata', myid, f'{mkpasswd("lpat1")}', act, 0, 600)
    s2 = co.savedObj('senddata', myid, f'{mkpasswd("lpat1")}', act, '1/1/2020', 300)
    s3 = co.savedObj('senddata', myid, f'{mkpasswd("lpat1")}', act, 20200101, 30)
    s4 = co.savedObj('senddata', myid, f'{mkpasswd("lpat1")}', act, '1/1/2020', 161)
    '''
    to update namedtuple field use:
           s1._replace(maxrecs=400)
    '''

    pd.set_option('display.max_rows', None)

    sobj = s3

    yn = input(f'retrieve {sobj.maxrecs} records for user {userid} from {act} ')
    if yn.startswith('y'):
        df = req_saved_data(sobj)
        print(f'df has {len(df)} records')
        df.tail()