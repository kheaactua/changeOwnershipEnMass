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
		cur.execute("CREATE TABLE files(id text PRIMARY KEY, Host INT, uid INT, gid INT, file TEXT, changed char(1) default '0')")
		con.commit();

	# Cache host map
	cur.execute("SELECT id,hostname FROM hosts")
	rows = cur.fetchall()

	host_ids={}
	for row in rows:
		host_ids[row[1]] = row[0];
	
	print(host_ids)

	for f in fp:
		f=f.strip()
		if i == 0:
			base = f
			i+=1
			continue

		if os.path.isfile(f) is False:
			continue

		# Strip base
		f = re.sub(r"^%s"%base, "", f);
		# Ensure there's a / at the front
		if f[0] is not '/':
			f="/%s"%f

		#print("File: %s, mode: "%(f))
		mode = os.stat(f)
		#print("File: %s, uid=%d, gid=%d "%(f, mode[ST_UID], mode[ST_GID]))
		cur.execute("INSERT INTO files (id, host, uid, gid, file) VALUES('%s', %d, %d, %d, '%s')"%(genKey(f), host_ids[hostname], mode[ST_UID], mode[ST_GID], f))

		con.commit();
		i+=1

def main():


	try:
		opts, args = getopt.getopt(sys.argv[1:], 'd:g:vh:')
	except getopt.GetoptError as err:
		# print help information and exit:
		print(str(err)) # will print something like "option -a not recognized"
		print("Usage: \
	-g <file of files> \
		Load lsit of files into DB, file is a list of files.  First line should be the \"base directory\" \
		For example, for /pool0/backups/dena/data/file, first line in file should be /pool0/backups/dena/ \
 \
	-v \
		verbose \
 \
	-d <db file> \
 \
	-h <hostname> \
		Default: %s \
"%(socket.gethostname()));
		sys.exit(2)


	file_db = 'files.db'
	hostname = socket.gethostname()
	for o,a in opts:
		if o == "-d":
			file_db = a;
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
		#else:
		#	assert False, "unhandled option"



#		
#		if con:
#			con.close()


if __name__ == "__main__":
	main()
