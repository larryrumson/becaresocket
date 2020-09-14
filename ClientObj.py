#import hashlib
import hashlib
import time
import json
from collections import namedtuple

def timefmt(t, z='local'):
    if z == 'local':
        mytime = time.strftime("%r", time.localtime(t))
    else:
        mytime = time.strftime("%r", time.gmtime(t))
    return mytime

def do_hash(str):
    hashobj = hashlib.md5(str.encode())
    return hashobj.hexdigest()

class DataObj:
    def __init__(self, _id, _passwd, _jdata, _act):
        self.id = _id
        self.passwd = _passwd
        self.jdata = _jdata
        self.action = _act

class ClientObj:
    def __init__(self, _conn=None,_tid=-1, _raddr=''):
        self.conn = _conn
        self.tid = _tid
        self.raddr = _raddr
        self.stime = time.time()
        self.active = True
        self.jstr = '[]'        # json encoding of data string
        self.table = ''
        self.userid = ''

    
    def pp(self):
        tf = timefmt(self.stime)
        s1 = f'client {self.tid} {self.userid} stime={tf}  active={self.active} conn={self.conn} '
        return s1

def tuple2ClientObj(t):                                 # convert namedtuple to ClientObj
    c = ClientObj(t.connection, t.tid, t.raddr)
    return c

def class2jsn(cl):

    s= json.dumps(cl, default=lambda x: x.__dict__)
    return s

loginObj = namedtuple('loginObj', 'action userid passwd fname lname gender')

dataObj = namedtuple('dataObj', 'action dbid passwd activity data cols recs')

# dont change above since already in use
# used by clientsock to request data
savedObj = namedtuple('savedObj', 'action dbid passwd activity bdate maxrecs')

#used by sockserver to send data to client
sendObj = namedtuple('sendObj', 'action uid bdate activity cols recs strlen')

requestObj = namedtuple('requestObj', 'action dbid passwd activity')
