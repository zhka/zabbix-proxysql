#!/usr/bin/python
# -*- coding: utf-8
#
# Zabbix-template for monitoring ProxySQL
# Konstantin A Zhuravlev aka ZhuKoV <zhukov@zhukov.int.ru> 2018
# 
# Usage: $0 discovery <servers|hostgroups>
#	$0 get <server|hostgroup|proxysql> [object_id]

############################################################

proxysql_host     = "127.0.0.1"
proxysql_port     = 6032
proxysql_user     = "root"
proxysql_password = "root"

############################################################

import sys
import json
import MySQLdb
import itertools

class proxysql:
	def __init__(self, proxysql_host, proxysql_port, proxysql_user, proxysql_password):
		self.__connection = MySQLdb.connect(host=proxysql_host, port=proxysql_port, user=proxysql_user, passwd=proxysql_password, db="main")
		self.__cursor = self.__connection.cursor()
	
	def __del__(self):
		self.__connection.close()

	def __select(self, sql):
		self.__cursor.execute(sql)
		#return self.__cursor.fetchall()
    		field_names = [d[0].lower() for d in self.__cursor.description]
		while True:
			rows = self.__cursor.fetchmany()
			if not rows: return
			for row in rows:
	    			yield dict(itertools.izip(field_names, row))

	def get_servers(self):
		return self.__select("""SELECT 	`hostname`,
						`port`
					FROM `runtime_mysql_servers`
					GROUP BY `hostname`, `port`;
		""")
	def get_hostgroups(self):
		return self.__select("""SELECT	'writer' AS 'role',
						`writer_hostgroup` AS 'id'
					FROM `runtime_mysql_group_replication_hostgroups`
					UNION
					SELECT	'backup_writer' AS 'role',
						`backup_writer_hostgroup` AS 'id'
					FROM `runtime_mysql_group_replication_hostgroups`
					UNION
					SELECT	'reader' AS 'role',
						`reader_hostgroup` AS 'id'
					FROM `runtime_mysql_group_replication_hostgroups`
					UNION
					SELECT	'offline' AS 'role',
						offline_hostgroup AS 'id'
					FROM `runtime_mysql_group_replication_hostgroups`
					UNION
					SELECT  'writer' AS 'role',
						`writer_hostgroup` AS 'id'
					FROM `runtime_mysql_replication_hostgroups`
					UNION
					SELECT  'reader' AS 'role',
						`reader_hostgroup` AS 'id'
					FROM `runtime_mysql_replication_hostgroups`;
		""")
	def get_all_command_counters(self):
		return self.__select("""SELECT	`Command`,
						`Total_cnt`, 
						`cnt_100us`,
						`cnt_500us`,
						`cnt_1ms`,
						`cnt_5ms`,
						`cnt_10ms`,
						`cnt_50ms`,
						`cnt_100ms`,
						`cnt_500ms`,
						`cnt_1s`,
						`cnt_5s`,
						`cnt_10s`,
						`cnt_INFs`
					FROM `stats`.`stats_mysql_commands_counters`
					WHERE `Command` in ('COMMIT','ROLLBACK','SET','START_TRANSACTION','SELECT','INSERT','UPDATE','DELETE','SHOW_TABLE_STATUS','SHOW');
		""")

	def get_connstat_of_server(self, host, port):
		return self.__select("""SELECT	`status`,
						SUM(`ConnUsed`) AS 'connused',
						SUM(`ConnFree`) AS 'connfree',
						SUM(`ConnOK`) AS 'connok',
						SUM(`ConnERR`) AS 'connerr',
						SUM(`Queries`) AS 'queries',
						SUM(`Bytes_data_sent`) AS 'sent',
						SUM(`Bytes_data_recv`) AS 'recv',
						Latency_us
					FROM `stats`.`stats_mysql_connection_pool`
					WHERE `srv_host` = '%s' AND `srv_port` = '%s';
		""" % (host, port) )

def print_help():
	print "\nUsage:\t%s discovery <servers|hostgroups>\n\t%s get <server|hostgroup|proxysql> [object_id]\n" % (sys.argv[0], sys.argv[0])

if len(sys.argv) <= 2:
	print_help()
	sys.exit(1)

pconn = proxysql(proxysql_host, proxysql_port, proxysql_user, proxysql_password)

if sys.argv[1] == 'discovery':
	discovery = {"data":[]}
	if sys.argv[2] == 'servers':
		for server in pconn.get_servers():
			discovery["data"].append({"{#SERVERNAME}":server['hostname'], "{#SERVERPORT}":server['port']})
		print json.dumps(discovery, indent=2, sort_keys=True)
		sys.exit(0)
	elif sys.argv[2] == 'hostgroups':
		for hostgroup in pconn.get_hostgroups():
			discovery["data"].append({"{#HOSTGROUPID}":hostgroup['id'], "{#HOSTGROUPROLE}":hostgroup['role']})
		print json.dumps(discovery, indent=2, sort_keys=True)
		sys.exit(0)
	else:
		print_help()
		sys.exit(1)
elif sys.argv[1] == 'get':
	if sys.argv[2] == 'proxysql':
		stats = {'commands':{}}
		for c in pconn.get_all_command_counters():
			stats['commands'][c['command']] = c
		print json.dumps(stats, indent=2, sort_keys=True)
	elif sys.argv[2] == 'server':
		if len(sys.argv) <= 4:
			print_help()
			sys.exit(1)
		else:
			stats = {"connstat":{}}
			for c in pconn.get_connstat_of_server(sys.argv[3], sys.argv[4]):
				stats['connstat'] = c
			print json.dumps(stats, indent=2, sort_keys=True)
	elif sys.argv[2] == 'hostgroup':
		if len(sys.argv) <= 3:
			print_help()
			sys.exit(1)
		else:
			print "4"
	else:
		print_help()
		sys.exit(1)
	sys.exit(0)
