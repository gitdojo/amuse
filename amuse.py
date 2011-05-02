#!/usr/bin/env python
from optparse import OptionParser
import sys
import time
import threading
import Queue
import random
import pexpect

# Script options
usage = """%prog [options]"""
opts = OptionParser(usage=usage)
opts.add_option("-f",  dest="filename", default="acquia_drupal.conf",
        help="Configuration file.", metavar="FILE")
opts.add_option("-r",  dest="runtime", type="int", default=30,
        help="Run time (secs). Default=60.", metavar="SECS")
opts.add_option("-v", dest="verbose", default=False, action="store_true",
        help="Set verbose mode")
(options, args) = opts.parse_args()

# Set default values
if not options.filename: 
    print "Mandatory option 'configuration filename' is missing."
    sys.exit(1)

# ==============================================================================================
class Group:
# ==============================================================================================
    def __init__(self, id, nusers, nsessions, uname, pwd):
        self.id = id
        self.num_users = nusers
        self.num_sessions = nsessions
        self.uname = uname
        self.pwd = pwd
        # Create a queue for each group of users:sessions
        self.queue = Queue.Queue(maxsize=nusers)
        # Create lists for queries, counts (i.e. weights) and think-times
        self.queryId = []
        self.queryFile = []
        self.queryCnt = []
        self.queryZZZ = []

    def addquery(self, id, file, count, zzz):
        self.queryId.append(id)
        self.queryFile.append(file)
        self.queryCnt.append(count)
        self.queryZZZ.append(zzz)

    def weighted_choice(self, total_count):
        n = random.uniform(0, total_count)
        for qq, fn, cnt, zzz in zip(self.queryId, self.queryFile, self.queryCnt, self.queryZZZ):
            if n < cnt:
                return qq, fn, random.uniform(0.0, 2.0*zzz)
            n = n - cnt
        return qq, fn, random.uniform(0.0, 2.0*zzz)

# ==============================================================================================
class User(threading.Thread):
# ==============================================================================================
    def __init__ (self, group, ix0):
        threading.Thread.__init__(self)
        self.id = "%s_user%d" % (group.id, ix0)
        self.group = group
        self.total_count = sum(group.queryCnt)
        self.res_event = threading.Event()

    def run(self):
        for self.seq in range(self.total_count):
            qq, fn, zzz = self.group.weighted_choice(self.total_count)
            if options.verbose:
                print "User %s sleeping for %.2f seconds at  %s" % (self.id, zzz, time.ctime())
            time.sleep(zzz)
            self.queryId = qq
            self.request = "\. %s/%s" % (amuseDict["sqldir"], fn)
            if options.verbose:
                print "User %s sent query %s request %s at %s" % (self.id, qq, self.request, time.ctime())
            self.group.queue.put(self)
            self.res_event.wait()
            if options.verbose:
                print "User %s got response (%s) for query %s at %s" % (
                  self.id, self.response, qq, time.ctime())
            self.res_event.clear()
            if time.time() > amuseDict["end_time"]:
                break

# ==============================================================================================
class Session(threading.Thread):
# ==============================================================================================
    def __init__ (self, group, ix0):
        threading.Thread.__init__(self)
        self.id = "session_%d" % ix0
        self.group = group
        wfn = "%s/amuse.%s.%d.log" % (amuseDict["resdir"], amuseDict["db"], ix0)
        self.log = open(wfn, 'w')
        self.prompt = "mysql> "
        wcmd = "mysql -s -u %s " % (group.uname)
        if group.pwd:
            wcmd += "-p%s " % (group.pwd)
        wcmd += amuseDict["db"]
        self.logit(wcmd)
        self.pexp = pexpect.spawn(wcmd)
        wresp = self.do_expect()
        
    def run(self):
        while True:
            wuser = self.group.queue.get()
            wstart = time.time()
            self.pexp.sendline(wuser.request)
            wuser.response = self.do_expect()
            self.logit("Response time for user %s query %s # %d is %.3f seconds" %
                (wuser.id, wuser.queryId, wuser.seq, time.time() - wstart))
            wuser.res_event.set()

    def logit(self, msg):
        self.log.write("%s %s %s\n" % (time.ctime(), self.id, msg))

    def hit(self):
        if options.verbose:
            print ">>>>>>>>>> HIT (%s) BEFORE <<<<<<<<<<" % self.prompt
            print dump(self.pexp.before)
            print ">>>>>>>>>> HIT (%s) AFTER <<<<<<<<<<" % self.prompt
            print dump(self.pexp.after)

    def oops(self, otype):
        print ">>>>>>>>>> %s (%s) BEFORE <<<<<<<<<<" % (otype, self.prompt)
        print dump(self.pexp.before)
        if self.pexp.after != pexpect.EOF:
            print ">>>>>>>>>> %s (%s) AFTER <<<<<<<<<<" % (otype, self.prompt)
            print dump(self.pexp.after)
            print self.pexp.after
        sys.exit(1)

    def do_expect(self):
        if options.verbose:
            print "pexpect waiting for %s at %s" % (self.prompt, time.ctime())
        wrc = self.pexp.expect ([self.prompt, pexpect.TIMEOUT, pexpect.EOF])
        if wrc == 0:
            self.hit()
            resp = self.pexp.before
        elif wrc == 1:
            self.oops("TIMEOUT")
        else:
            self.oops("EOF")
        return resp

# ----------------------------------------------------------------------------------------------
def get_confile():
# ----------------------------------------------------------------------------------------------
    try:
        fi = open(options.filename, 'r')
    except IOError:
        print "Error opening conf file(%s)." % options.filename
        sys.exit()

    amuseDict["hostip"] = ''
    amuseDict["db"] = ''
    amuseDict["resdir"] = '.'
    amuseDict["sqldir"] = '.'
    for line in fi.readlines():
        fields = line.split()
        if len(fields) < 2: continue
        key = fields[0].lower()
        if key in ['//','#']: continue
        elif key in ["hostip","db","resdir","sqldir"]:
            amuseDict[key] = fields[1]
        elif key == "group":
            id = fields[1]
            if len(fields) == 5: pwd = None
            elif len(fields) == 6: pwd = fields[5]
            else:
                print "Error: group definition requires at least 5 tokens"
                sys.exit(1)
            cur_group = Group(id, int(fields[2]), int(fields[3]), fields[4], pwd)
            groupDict[id] = cur_group
        elif key == "query":
            if len(fields) != 5:
                print "Error: query definition requires exactly 5 tokens"
                print "Example: query main1 main_page.sql 100 5"
                sys.exit(1)
            cur_group.addquery(fields[1], fields[2], int(fields[3]), int(fields[4]))
        else:
            print "Error: invalid keyword (%s) in conf file line (%s)" % (key, line)
            sys.exit(1)

    fi.close()

# ----------------------------------------------------------------------------------------------
def dump(src, length=16):
# ----------------------------------------------------------------------------------------------
    FILTER=''.join([(len(repr(chr(x)))==3) and chr(x) or '.' for x in range(256)])
    N=0; result=''
    while src:
       s,src = src[:length],src[length:]
       hexa = ' '.join(["%02X"%ord(x) for x in s])
       s = s.translate(FILTER)
       result += "%04X   %-*s   %s\n" % (N, length*3, hexa, s)
       N+=length
    return result

# ==============================================================================================
# Start here
# ==============================================================================================
amuseDict = {}
groupDict = {}
get_confile()
# Start all sessions first so they have time to connect
for uname in groupDict.keys():
    group = groupDict[uname]
    for ix0 in range(group.num_sessions):
        ts = Session(group, ix0)
        ts.daemon = True
        ts.start()


amuseDict["end_time"] = time.time() + options.runtime
for uname in groupDict.keys():
    group = groupDict[uname]
    for ix0 in range(group.num_users):
        tu = User(group, ix0)
        tu.start()

# Join queue for group in last session created.
ts.group.queue.join()

