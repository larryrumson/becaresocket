import sys
import os
import pymysql.cursors
import pandas as pd
import readline
import json
from json.decoder import JSONDecodeError
from collections import namedtuple
from colorama import init, Fore, Back, Style
import time
import pymysql.err
from pymysql.err import OperationalError
from pathlib import Path

yellow = Fore.YELLOW
green = Fore.GREEN

maxdur = 15000

activities = [ 
    "ArmElevation", "SixMinutes", "TwentyFiveSteps", "UpAndGo", 
    "Snooker", "Path2", "ContrastSensitivity", "TranscriptionTest", 
    "StroopTest", "TapTask", "CognitiveTest", "MemoryTest", "Vibration" 
]

def testtable(sact):                           # testable names
    tab = ''
    if not sact in activities:
        n = strindex(sact, 'Six')
        if n  >= 0: tab = 'MySix'
        else:
            print(f'{sact} is not a valid activity n={n}')
    else:
        if sact == 'ArmElevation' : tab = 'MyArm'
        elif sact == 'SixMinutes' : tab = 'MySix'
        elif sact == 'TwentyFiveSteps' : tab = 'MyT25'
        elif sact == 'Path2' : tab = 'MyPath'
        elif sact == 'UpAndGo' : tab = 'MyUp'
        elif sact == 'Snooker' : tab = 'MySnook'
        elif sact == 'ContrastSensitivity' : tab = 'MyCon'
        elif sact == 'MemoryTest' : tab = 'MyMem'
        elif sact == 'Vibration' : tab = 'MyVib'
        elif sact == 'CognitiveTest' : tab = 'Cog'
        elif sact == 'StroopTest' : tab = 'MyStroop'
        elif sact == 'TapTask' : tab = 'MyTap'
        elif sact == 'TranscriptionTest' : tab = 'MyTran'
    return tab

def testTableList(actlist=activities):         # get list of testtable for actlist
    l =[]
    for a in actlist:
        l.append(testtable(a))
    return l

def hf(file):
    fp = open(file, 'w')
    for i in range(readline.get_current_history_length()):
        fp.write(readline.get_history_item(i + 1))
        fp.write('\n')
    fp.close()

def hsearch(str, limit=10):                # search history for str
    last = readline.get_current_history_length()
    hr = range(1, last)
    cnt = 0 
    for i in reversed(hr):
        hline = readline.get_history_item(i)
        if hline.find(str) > 0:
            print('%d  %s' % (i, hline) )
            cnt += 1
            if cnt >= limit:
                break

def save_var(keyval, dval):             #save variable in global dictionary
    global vardict
    try: vardict
    except:
        vardict = {}
    vardict[keyval] = dval

def restore_var(keyval):             # retrieve variable in global dictionary
    global vardict
    if keyval in vardict:
        return vardict[keyval]

def undef(keyval):
    global vardict
    try: vardict
    except:
        vardict = {}
    if (not keyval in vardict):
        print('loading %s' % (keyval))
        yn = True
    else:
        yn = False
    return yn

if undef('act'):
    act = "ContrastSensitivity"
    act = "TranscriptionTest"
    act = "CognitiveTest"
    act = "MemoryTest"
    act = "TwentyFiveSteps"
    act = "ArmElevation"
    save_var('act', act)
    print(f'dbfuns: act={act}')

if not dir().count('conn'):
    conn = None
    curr = None

print(f'mysql.py using {act} activity')

def strindex(str, pat):
    n = -1
    try:
        n = str.index(pat) 
    except:
        pass
    return n

####################  mysql functions           ########################################
def dbcred_save(conn, _host, _port, _user, _passwd):
    save_var('conn', conn)
    save_var('dbhost', _host)
    save_var('dbport', _port)
    save_var('dbuser', _user)
    save_var('dbpass', _passwd)

def dbcred_restore():
    dbhost = restore_var('dbhost')
    dbport = restore_var('dbport')
    dbuser = restore_var('dbuser')
    dbpass = restore_var('dbpass')
    return [dbhost, dbport, dbuser, dbpass]

def conndb(_host='192.168.1.40', _port=3306, _user='lrubin', _passwd='vision11', _db='msdata'):
    global conn, cur                                #first time connection
    print(f"connecting to db host {_host}")
    conn = pymysql.connect(host=_host, port=_port, user=_user, password=_passwd, database=_db)
    cur = conn.cursor()
    dbcred_save(conn, _host, _port, _user, _passwd)
    return conn

def connect():                                # reconnect
    lst = dbcred_restore()
    print('reconnecting to db')
    global conn, cur
    conn = conndb()
    cur = conn.cursor()
    save_var('conn', conn)

def close():
    global conn, cur
    cur.close()
    conn.close
    cur = conn = None
    print('db connection closed')

def save_conn():
    global conn, cur
    conn = conndb()
    cur = conn.cursor()
    save_var('conn', conn)
    save_var('cur', cur)
    save_var('activities', activities)

def dexec(cmd, all=False):                    # exec string return list(dboutput)
    global cur, conn
    if not 'conn' in vardict:
        print('dexec: start connection')
        save_conn()

    dlist = []
    rows = 0
    myerr = False
    try:
        cur.execute(cmd)
        rows = cur.fetchall()
    except pymysql.err.OperationalError as e:
        print(f'mysql operational error: {e}')
        print("reconnecting to db")
        connect()
        myerr = True
    except AttributeError as e:
        print(f'mysql operational error: {e}')
        print("reconnecting to db")
        connect()
        myerr = True
    if all:
        return rows

    for l in rows:
        dlist.append(l[0])
    return dlist

def tables():                                # all tables in msdata
    return dexec("show tables")

def headings(dbtable):                       # table headings
    cmd = "describe " + dbtable
    return dexec(cmd)

def query2list(cmd):                         # multiline output list
    tuples = dexec(cmd, True)
    dlist = list(tuples)
    return dlist

lastcmd = ''
act = ''

def rows2df(dlist, myact):                      # pretty print tuples from dexec()
    #dlist = list(rowlist)
    cols = headings(myact)
    df = pd.DataFrame(dlist, columns=cols)
    return df

def query2df(cmd, cols):                  # output df from query, supply col names
    dlist = query2list(cmd)
    df = pd.DataFrame(dlist, columns=cols)
    return df

def table2df(table='Edss',dir='/tmp', dosave=True):
    cols= headings(table)
    cmd = 'select * from ' + table
    df = query2df(cmd, cols)
    if dosave:
        fname = Path(dir) / table
        df.to_csv(fname, index=False)
    return df

alltables = None

def sel(userid, table=act, recs=0, date=0, time=0, doprint=False):
    global lastcmd, alltables                               # global lastcmd for debugging
    if alltables == None:
        alltables = tables()
    if not table in alltables:
        if len(table) == 0:
            print('table is blank')
        else:
            print(f'dbsel: {table} not a db table')
        return pd.DataFrame()

    cmd = f'select * from {table} where userid="{userid}" '
    lastcmd = cmd
    if date > 0:
        cmd = cmd + f' and date="{date}"'
    if time > 0:
        cmd = cmd + f' and time="{time}"'
    #cmd = cmd + " order by date,time"
    if recs > 0:
        cmd = cmd + f' limit {recs} '
    #cmd = f'select * from {table} where userid="{userid}" and date="{date}" and time="{time}"'
    if doprint: print(f'cmd={cmd}')
    dlist =  query2list(cmd)
    df = rows2df(dlist, table)
    return df

def dbid2userid(id):
    res = ''
    id = int(id)
    cmd = f"select * from Users where id = {id} "
    dlist = query2list(cmd)
    if len(dlist) == 0:
        cmd = f"select * from Fusers where id = {id} "
        dlist = query2list(cmd)
    if len(dlist) > 0:
        tup = dlist[0]
        res = tup[0] if tup[4] == id else ''
    return res

def find(srow):                          # srow is a string to be used as sel inputs
    slist = srow.split()
    print(slist)
    tab = slist[0]
    date = int(slist[1])
    time = int(slist[2])
    userid = slist[3]
    return sel(userid, date, time, tab)

def args2cmd(userid, bdate, edate, table):
    global lastcmd
    if len(userid) > 0:
        cmd = f'select * from {table} where userid="{userid}"'
    else:
        cmd = f'select * from {table}' 
    if bdate > 0 and edate > 0:
        cmd = cmd + f' and date between {bdate} and {edate}'
    elif bdate > 0:
        cmd = cmd + f' and date >= {bdate}'
    elif edate > 0:
        cmd = cmd + f' and date <= {edate}'
    lastcmd = cmd
    return cmd

def dbload(userid, table, bdate=0, edate=0, doprint=False):           #load data from db
    global lastcmd                                              # global lastcmd for debugging
    if doprint: print(f'dbsel: act={act}')
    cmd = args2cmd(userid, bdate, edate, table)
    if doprint: print(f'cmd={cmd}')
    dlist =  query2list(cmd)
    df = rows2df(dlist, table)
    return df

def loaddf(userid, bdate, edate, table, n=0):
    global conn
    cmd = args2cmd(userid, bdate, edate, table)
    af = pd.read_sql(cmd, conn)
    return af

def timeload(userid, bdate, edate, table, n=0):
    starttime = time.time()
    for i in range(0,n+1):
        af = dbload(userid, table, bdate, edate, )
    endtime = time.time()
    dtime = endtime - starttime
    print("testload time taken for %d loads = %.3f " % (n+1, dtime))
    return af

def timeloaddf(userid, bdate, edate, table, n=0):
    global conn
    cmd = args2cmd(userid, bdate, edate, table)
    starttime = time.time()
    for i in range(0,n+1):
        af = pd.read_sql(cmd, conn)
    endtime = time.time()
    dtime = endtime - starttime
    print("load df reaad_sql time taken for %d loads = %.3f " % (n+1, dtime))
    return af

dbcols = ['activityname', 'date', 'time', 'userid']

def unique(table, cols=dbcols):     # unique <act,date,time,user> from table
    tablist = tables()
    if table in tablist:
        cmd = 'select distinct activityname,Date,Time,userid from ' + table
        dlist = query2list(cmd)
        df = pd.DataFrame(dlist, columns=cols)
    else:
        print(f'{table} doesnt exist')
        df = pd.DataFrame()
    return df

def table_sizes(filter=[]):                  # filter can be activities
    cmd = "select TABLE_NAME,TABLE_ROWS FROM `information_schema`.`tables` where `table_schema`='msdata'"
    dlist=query2list(cmd)
    df = pd.DataFrame(dlist, columns=['table', 'cnt'])
    if len(filter) > 0:
        df = df.query('table in @filter')
    return df

def all(cmd):             # send cmd to all activity tables :@t 
    global activities
    clist  = []
    if strindex(cmd, '@t') < 0:
        print('f{cmd} : missing @t')
        return
    for a in activities:
        acmd = cmd.replace('@t', a)
        print(acmd)
        rows = dexec(acmd, True)
        if len(rows)  == 0:
            print('  no rows from query')
        else:
            print(f' {len(rows)} from query')
            clist.append(rows)
    return clist

def fmt_clist(clist, onlyone=-1):              #format output of all
    for i,col in enumerate(clist):
        if onlyone > 0 and i != onlyone:
            continue
        print(f'-------------------{i}------------------')
        for c in col:
            print(c)

def table_users(tab):                    # distinct users in a table
    cmd = 'select distinct userid from ' + tab
    return dexec(cmd)

def tables_cmd(allcmd, tablist):         # send cmd to all tables in list
    clist  = []
    tlist =[]
    for t in tablist:
        cmd = allcmd
        cmd = cmd.replace('@t', t)
        #   print(f'after {cmd}')
        tlist.append(t)
        clist.append(dexec(cmd))
        #df = pd.DataFrame(clist, columns=tlist)
    return [tlist, clist]

def len_heading(df, dbtable):
    global vardict, cur                 #this wont pick up a change in table columns
    if dbtable not in vardict:
        vardict[dbtable] = headings(dbtable)

    lv = len(vardict[dbtable])
    lt = len(list(df))
    return lt, lv

def insert(df, table, name='idk', doprint=True):
    global conn, cur
    lt, lv = len_heading(df, table)
    cnt = 0
    if lt != lv:
        print(f'dbinsert error: {name}.df size={lt}  and {table} size={lv}')
        print(f'{table} : {vardict[table]} ')
        print(f'{df.head()} : {list(df)} ')
    else:
        rowlist = list(df.itertuples(index=False, name=None))
        for r in rowlist:
            if doprint: print(f"insert into {table} values {r}")
            cur.execute(f"insert into {table} values {r}")
            cnt += 1
    conn.commit()                                   # rows arent inserted until commit
    return cnt

def users2df():                                       # dataframe of users
    usertable = ['userid', 'fname', 'lname', 'gender', 'id']
    uall=dexec('select * from Users', True)
    uf =pd.DataFrame(list(uall), columns=usertable)
    return uf


def dict_order_flds(flds, fdict, dbcols, doprint=False):   #want flds in dbcol order
    s = ''
    for d in dbcols:
        if d in fdict.keys():
            n = fdict[d]
            val = flds[n]
            if doprint: print(f'fdict[{d}] = {val}')
            if len(s) == 0:
                s = val
            else:
                s = s + ',' + val
        else:
            print(f"dictorder: missing {d}")
    return s

def load_user_list(ulist):
    mydict = {}
    edss = -1
    for uname in ulist:
        if uname == '-':
            print(f'skipping {uname}')
            continue
        print(f'subject {uname}')
        u = Subject(uname, edss)
        mydict[uname] = u
    return mydict

##########################  end mysql functions #############################

def filesize(fpath):  
    bytes = -1
    if os.path.isfile(fpath):
        bytes = os.path.getsize(fpath)
    return bytes

def read_data(subject, activity, mydir='', doprint=False) :
    print(f'reading data from dir {mydir}')
    l = [ subject, activity, "csv"]
    str = '.'
    file = str.join(l)
    mypath = Path(mydir) / file
    data = pd.DataFrame()
    bytes = filesize(mypath)
    print(f'file={mypath} bytes={bytes} ')
    if  bytes > 0:
        df = pd.read_csv(mypath)
        df.rename(columns=lambda x : x.strip().lower(), inplace=True)
        df.rename(columns={'activity': 'activityname'}, inplace=True)
        #df['date']=pd.to_datetime(df['date'],format='%Y%m%d')
        data = df
    
    if doprint: print(f'{file} has {bytes} size')
    return data

doprint=False
showWarnings = True
PrintDetail=False

# pd.set_option('display.max_rows', None)

######################## filter data ############################

def df_reset_index(df):
    if 'level_0' in list(df):
        df=df.drop('level_0', axis=1)
    newdf = df.reset_index()
    return newdf

def filterdata(mydf, scnt=1.5, doprint=False):
    mean = mydf.dur.mean()
    std = mydf.dur.std()
    if doprint:
        print("dirty dur has mean %.2f std=%.2f" % (mean,  std))

    l = mean - std * scnt
    r = mean + std * scnt
    l = max(l, 0)
    newdf = mydf[(mydf.dur > l) & (mydf.dur < r)]
    newdf = df_reset_index(newdf)
    mean = newdf.dur.mean()
    std = newdf.dur.std()
    if doprint:
        print("clean dur has mean %.2f std=%.2f\n" % (mean,  std))
    return newdf

def filterlist(ulist, doprint=True):                  # List<subjectnames>
    for user in ulist:
        u = lookup(user)
        if u != None:
            print(u)
            u.filteract()
            if doprint: u.pp()
        else:
            print(f'filterlist {user} not found')


########################   end filter data ############################

def orderbydate(df, cols=['date', 'time', 'seq']):  # order the dataframe by cols
    dfcols = list(df)
    myorder = cols.copy()
    if 'hand' in dfcols:
        myorder.insert(2, 'hand')
    sf = df.sort_values(myorder, inplace=True)           #default is ascending
    return sf

def orderactivities(s):    
    for a in list(s.adict):
        #print(f'{a} {type(s.adict[a])}')
        orderbydate(s.adict[a])

##########################  order activities by date,time, seq

def replace_extreme_values(df, col='dur',lb=0, ub=maxdur, id='df', doprint=False):
    if col not in list(df):
        return
    uf = df.loc[df[col] > ub]
    cnt = len(uf)
    if cnt > 0:
        if showWarnings: print(f'{id}: {cnt} lines have {col} > {ub}')
        if doprint: print(uf)
        df[col].values[df[col].values > ub] = ub

    lf = df.loc[df[col] < lb]
    cnt = len(lf)
    if cnt > 0:
        if cnt > 10: print(f'{id}: {cnt} lines have {col} < {lb}')
        if doprint: print(lf)
        df[col].values[df[col].values < lb] = lb

def date_report(u):         # range of activity dates
    amax = 0
    amin = 30200101
    keys = u.adict.keys()
    for k in keys:
        ad = u.adict[k]
        dmin = min(ad.date.unique())
        dmax = max(ad.date.unique())
        amin = min(amin, dmin)
        amax = max(amax, dmax)
    return [amin, amax]

################   add blk column  #######################

def blist2blocks(dlist, mindiff=5):
    bn = 0 
    bl = []
    for d in dlist:
        if d > mindiff:  bn += 1
        bl.append(bn)
    return bl

################   end add blk column  #######################

def add_block_col(df):      # block col collects a seq.
    cols = list(df)
    if not "blk" in cols:
        pf = df.seq.shift(1) - df.seq       # series of diffs row[i] - row[i-1]
        dl = pf.fillna(0).to_list()
        bl =  blist2blocks(dl)
        df['blk'] = bl
    return df

def groupdf(df, col='blk'):        # group dataframe by column
    dlist = []                              # dataframe list
    vlist = []                              # values of split
    if col not in list(df):
        print(f'{col} missing from df')
        return dlist, vlist
    for v,d in df.groupby(col):
        vlist.append(v)
        dlist.append(d)
    return dlist, vlist

class Subject:
    edssdict = {}
    userdict = {}

    def __init__(self, name, bdate=0, edate=0, edss=-1, savedict=True, mydir=''):
        self.name = str.lower(name)
        self.edss = float(edss)
        self.adict ={}
        self.acnt = 0
        self.filtercnt = 0
        self.unfiltered = {}

        useDB = True if len(mydir) == 0 else False

        for a in activities:
            if useDB:
                data = dbload(name, a, bdate, edate, doprint)
                # dont order by date because this will ruin the sequences
                # orderbydate(data)
                id = name + " " + a
                replace_extreme_values(data, 'dur', 0, maxdur, id)
            else:
                data = read_data(self.name, a, mydir)

            if len(data) > 0:
                data = add_block_col(data)
                self.adict[a] = data
                self.acnt +=1
            
        if self.acnt == 0:
            print(f'{self.name} has no activities')
        if savedict:
            Subject.edssdict[name] = edss
            Subject.userdict[name] = self

    def pp(self):
        if self.edss == -1.0:
            print(f'Subject {self.name} missing edss')
        else:
            print("Subject %s has edss %.2f" % (self.name, self.edss))
        print('  i    act                rowcnt  blks    dur:min    25%     dur:avg     75%    dur:max     Dmin       Dmax')
        keys = self.adict.keys()
        i = 0
        for k in keys:
            ad = self.adict[k]
            alen = len(ad.date)
            blks = ad.iloc[-1].blk
            des = ad.dur.describe()
            amin = des['min']
            amean = des['mean']
            amax = des['max']
            a25 = des['25%']
            a75 = des['75%']
            dmin = min(ad.date.unique())
            dmax = max(ad.date.unique())
            print(' %2d   %-20s  %5d  %3d  %8.0f %7.0f %10.0f %8.0f %8.0f     %d   %d'%
                (i, k, alen, blks, amin, a25, amean, a75, amax, dmin, dmax) )
            i += 1
            #print(ad.describe())
            #slice_method1(ad, "seq")

    def __repr__(self):
        global PrintDetail
        n = len(self.adict)
        if PrintDetail: self.pp()
        dr = date_report(self)
        if self.filtercnt == 0:
            mystr = f'{self.name} has {n} activities : unfiltered {dr}'
        else:
            mystr = f'{self.name} has {n} activities : filtered {self.filtercnt} times {dr}'
        return mystr

    def idict(self, i):                   # get ith dictionary (ith activity in list)
        ad = {}
        if i < len(self.adict):
            a = activities[i]
            ad = self.adict[a]
        return ad

    def filteract(self, scnt=1.5, doprint=False):            # filter activities with too many outliers
        for k in self.adict.keys():
            if self.filtercnt == 0:
                self.unfiltered[k] = self.adict[k].copy()
            kdf = filterdata(self.adict[k], scnt, doprint)
            self.adict[k] = kdf
        self.filtercnt += 1

########################## end Subject class ##################################

def pstats_df(df, i, k, color=green, tag=''):     #df is Dataframe<Activity>
    alen = len(df.date)
    ulen = len(df.date.unique())
    des = df.dur.describe()
    amin = des['min']
    amean = des['mean']
    amax = des['max']
    a25 = des['25%']
    a75 = des['75%']
    tries = len(df.date.unique())
    print('%s %2d   %-20s  %5d  %3d  %8.0f %10.0f %10.0f %10.0f %10.0f     %3d  %s'%
        (color, i, k, alen, ulen,amin, a25, amean, a75, amax, tries, tag) )

def pfiltered(s):          # compare filtered to original
    print("Subject %s was filtered %d times" % (s.name, s.filtercnt))
    print('  i    act                   cnt  trys    dur:min      25%     dur:avg       75%    dur:max    tries')
    keys = s.adict.keys()
    i = 0
    for k in keys:
        pstats_df(s.adict[k],i, k, green, 'filtered')
        if s.filtercnt > 0:
            pstats_df(s.unfiltered[k],i, k, yellow, 'orig')
        i += 1
    print(green)

def filtercnt(s, n):
    for i in range(n):
        s.filteract()

def lookup(id, mydict=Subject.userdict):
    sub = None
    if id in mydict:
        sub = mydict[id]
    return sub
    ######## read & write Json files

def jfile(data, file='/tmp/data.json'):             # write json file
    with open(file, 'w+') as f:
        json.dump(data, f)

def rfile(file='/tmp/data.json'):                   # read json file into string
    data = None
    with open(file, 'r') as f:
        try:
            data =json.load(f)
        except json.decoder.JSONDecodeError as e:
            print("got exep")
            print(e)

    return data

def jstr2Obj(jstr, objname='Obj'):         # jsn str to named tuple
    myobj = None
    try:
        myobj = json.loads(jstr, object_hook=lambda d: namedtuple(objname, d.keys())(*d.values()))
    except JSONDecodeError as e:
        print(f' json error: {e}')
    except TypeError:
        print(f' json type error: {e}')
    return myobj

def rlist(file):                                # read a list from file
    with open(file) as f:
        lineList = f.readlines()
        return lineList

    ######## end read & write Json files

def dict2db(mydict,tab='SPY'):
    global conn
    values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in mydict.values())
    dbcols = ', '.join(mydict.keys())
    #dbcols = dbcols.replace('TT', 'time')
    stmt = "INSERT INTO %s ( %s ) VALUES ( %s )" % (tab, dbcols, values)
    dexec(stmt)
    conn.commit()  
    return stmt

def prepare_df(df):             # prepare  df to send to server
    rf = df.copy()
    if 'userid' in rf:
        del rf['userid']
    if 'activityname' in rf:
        del rf['activityname']
    return rf

def restore_df(df, act, userid):
    jf = df.copy()
    if not 'userid' in jf:
        jf['userid'] = userid
    if not 'activityname' in jf:
        jf['activityname'] = act
    cols = headings(act)
    jf = jf.reindex(columns=cols)
    return jf

print('dbfuns.py done')