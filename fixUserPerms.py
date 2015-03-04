#!/usr/bin/env python

from __future__ import print_function

import sqlite3 as lite
import os, sys
from stat import *
import getopt
import socket
import re, hashlib

con = None

def genKey(fname):
    m = hashlib.md5()
    m.update(fname);
    return m.hexdigest();

def getHostCache(con):
	# Cache host map
	cur = con.cursor()
	cur.execute("SELECT id,hostname FROM hosts")
	rows = cur.fetchall()

	host_ids={}
	for row in rows:
		host_ids[row[1]] = row[0]

	#print(host_ids)
	return host_ids

def loadFiles(con, fname, hostname):
	""" Iterate through this file, and insert them into the db """
	fp = open(fname, 'r')

	i = 0
	base='';

	cur = con.cursor()
	# Put this in an init function
	cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='hosts'");
	if len(cur.fetchall()) < 1:
		cur.execute("CREATE TABLE hosts(id INT PRIMARY KEY, hostname text)")
		cur.execute("INSERT INTO hosts (id, hostname) VALUES(1, 'dena')")
		cur.execute("INSERT INTO hosts (id, hostname) VALUES(2, 'khea')")
		cur.execute("INSERT INTO hosts (id, hostname) VALUES(3, 'sahand')")
		cur.execute("INSERT INTO hosts (id, hostname) VALUES(4, 'sabalan')")
		con.commit();

	cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='files'");
	if len(cur.fetchall()) < 1:
		cur.execute("CREATE TABLE files(id text PRIMARY KEY, Host INT, old_uid INT, old_gid INT, new_uid INT, new_gid INT, file TEXT, changed char(1) default '0')")
		con.commit();

	# Cache host map
	host_ids = getHostCache(con)

	for f in fp:
		f=f.strip()
		#if i == 0:
		#	base = f
		#	i+=1
		#	continue

		if os.path.isfile(f) is False:
			continue

		## Strip base
		#f = re.sub(r"^%s"%base, "", f);
		## Ensure there's a / at the front
		#if f[0] is not '/':
		#	f="/%s"%f

		# First, check to see if it's in the DB
		cur.execute("SELECT file FROM files WHERE file='%s'"%f);
		if len(cur.fetchall()) == 1:
			continue;

		#print("File: %s, mode: "%(f))
		mode = os.stat(f)
		#print("File: %s, uid=%d, gid=%d "%(f, mode[ST_UID], mode[ST_GID]))
		cur.execute("INSERT INTO files (id, host, old_uid, old_gid, file) VALUES(?, ?, ?, ?, ?)", (genKey(f), host_ids[hostname], mode[ST_UID], mode[ST_GID], f))

		con.commit();
		i+=1

def loadMap(file_map):
	rem = re.compile(r"(g|u):(\d+)=>(\d+)");

	map_user = {}
	map_group = {}

	fp = open(file_map, 'r')
	for line in fp:
		parts = rem.match(line);
		#print(parts.group(1,2,3))
		if parts.group(1) == "u":
			#map_user.append({'old_id': int(parts.group(2)), 'new_id': int(parts.group(3))})
			map_user[parts.group(2)] = int(parts.group(3))
		else:
			#map_group.append({'old_id': int(parts.group(2)), 'new_id': int(parts.group(3))})
			map_group[parts.group(2)] = int(parts.group(3))

	#print(str(map_user));
	#print(str(map_group));

	return {'user': map_user, 'group': map_group}

def newId(pmap, oldId):
	""" Probably a WAY better way to do this """
	for p in pmap:
		if p['old_id'] == oldId:
			return p['new_id']

	return None

def changeFilePerms(con, perm_map, hostname, dryrun):
	# Cache host map
	host_ids = getHostCache(con)

	#print(perm_map)

	cur = con.cursor()
	cur.execute("SELECT id,file,old_uid,old_gid FROM files WHERE host='%s' AND changed='0'"%(host_ids[hostname]));
	#print("SELECT id,file,old_uid,old_gid FROM files WHERE host='%s' AND changed='0'"%(host_ids[hostname]));
	rows = cur.fetchall()
	for row in rows:
		f = row[1]
		id = row[0]
		old_uid = int(row[2])
		old_gid = int(row[3])

		# Double check that the old_uid and old_gid are right, just as a further check
		mode = os.stat(f)
		if mode[ST_UID] != old_uid:
			print("[Error]: Detected UID (%d) and recorded UID (%d) do not match! %s"%(mode[ST_UID], old_uid, f))
			continue;
		if mode[ST_GID] != old_gid:
			print("[Error]: Detected GID (%d) and recorded GID (%d) do not match! %s"%(mode[ST_GID], old_gid, f))
			continue;

		# Make sure we have a full map
		if str(old_uid) not in perm_map['user']:
			print("[Error]: No map for UID=%d!"%(old_uid))
			continue;
		if str(old_gid) not in perm_map['group']:
			print("[Error]: No map for GID=%d!"%(old_gid))
			continue;
		new_uid = perm_map['user'][str(old_uid)];
		new_gid = perm_map['group'][str(old_gid)];

		# Doing well if we got here
		print("[UID:%d=>%d,GID:%d=>%d] chown %d:%d %s"%(old_uid,new_uid, old_gid,new_gid, new_uid,new_gid, f))
		if dryrun is False:
			os.lchown(f, new_uid, new_gid)
			cur.execute("UPDATE files SET new_uid=%d, new_gid=%d, changed='1'"%(new_uid, new_gid));

	if dryrun is False:
		con.commit();


def main():

	try:
		opts, args = getopt.getopt(sys.argv[1:], 'd:g:vh:m:cC')
	except getopt.GetoptError as err:
		# print help information and exit:
		print(str(err)) # will print something like "option -a not recognized"
		print("Usage: \
 \
Iterates through a list of files (-g) altering the UID and GID \
of the file as shown in a map file (-m).  Each filename is \
stored in a database with a flag of whether the permissions \
have been changed.  This way a this script may be re-run to  \
your heart's content without worry that permissions will get \
continually changed (1000 to 1002, then 1002 to 1004, etc.) \
 \
Original import permissions are also recorded, so this operation \
is reversible. (not yet implemented) \
 \
Steps, first use the script to load the files with -g.  Then use \
it to actually perform the changes (-m and -c) \
 \
	-g <file of files> \
		Load list of files into DB, file is a list of files. \
 \
	-v \
		verbose \
 \
	-d <db file> \
 \
	-h <hostname> \
		Default: %s \
 \
 	-m <map file> \
 		Map file of UID's. Each line should have the format 'UserOrGroup:OldUid=>NewUid'.  i.e. 'u:1000=>1002', or 'g:500=>678' \
  \
  	-c \
  		Dry-run of permission changing \
  \
  	-C \
  		Actually change permissions \
"%(socket.gethostname()));
		sys.exit(2)


	file_db = 'files.db'
	file_map = 'map.txt'
	perm_map = {}
	hostname = socket.gethostname()
	for o,a in opts:
		if o == "-d":
			file_db = a;
		elif o == "-m":
			perm_map = loadMap(a);
		elif o == "-h":
			hostname = a;
		#else:
		#	assert False, "unhandled option %s"%(o)

	try:
		con = lite.connect(file_db)

		cur = con.cursor()    
		cur.execute('SELECT SQLITE_VERSION()')

		data = cur.fetchone()

		print("Connected to %s with SQLite version: %s"%(file_db, data))

	except lite.Error, e:

		print("Error %s:" % e.args[0])
		sys.exit(1)

	#finally:


	for o,a in opts:
		if o == "-g":
			loadFiles(con, a, hostname);
		elif o == "-c":
			changeFilePerms(con=con, perm_map=perm_map, hostname=hostname, dryrun=True)
		elif o == "-C":
			changeFilePerms(con=con, perm_map=perm_map, hostname=hostname, dryrun=False)
		#else:
		#	assert False, "unhandled option"



#		
#		if con:
#			con.close()


if __name__ == "__main__":
	main()
