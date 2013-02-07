#Depends on web.py, psycopg2, PyYaml, pytz
import web, psycopg2
import json, yaml, xmlrpclib
import datetime, pytz
from dblogin import dbname, dbuser, dbpass

categories = {}
#The temptation to name this cat_herder was extraordinary
def cat_handler(category):
	def inner(cl):
		categories[category] = cl
		return cl
	return inner

def xmldump(params):
	return xmlrpclib.dumps(tuple(params))

returntypes = {
	'json': json.dumps,
	'yml': yaml.dump,
	'xml': xmldump,
}

ctypes = {
	'json': 'application/json',
	'yml': 'text/x-yaml:',
	'xml': 'application/xml',
}

class category_handler:
	def GET(self, category, returntype='json'):
		args = web.input()
		if category in categories and returntype in returntypes:
			web.header('Content-Type', ctypes[returntype])
			return returntypes[returntype](categories[category].handle_category(args))
		else:
			return web.NotFound()

class object_handler:
	def GET(self, category, obj, returntype='json'):
		args = web.input()
		if category in categories and returntype in returntypes:
			return returntypes[returntype](categories[category].handle_object(obj, args))
		else:
			return web.NotFound()

class base_handler:
	conn = psycopg2.connect(database=dbname, user=dbuser, password=dbpass)
	cur = conn.cursor()
	@classmethod
	def handle_category(self, args):
		return web.NotFound()
	@classmethod
	def handle_object(self, obj, args):
		return web.NotFound()

@cat_handler('events')
class event_handler(base_handler):
	argsql = {
		'from': 'time>=%s',
		'until': 'time<=%s',
		'player': 'LOWER(player_name)=LOWER(%s)',
		'online': 'online=%s',
	}
	@classmethod
	def handle_category(self, args):
		#I don't know how to SQL, this is my attempt at a dynamic query
		sql = 'SELECT * FROM skynet_events'
		first = True
		params = []
		for key, value in args.iteritems():
			if key in self.argsql:
				if not first:
					sql+=' AND '+self.argsql[key]
					params.append(value)
				else:
					sql+=' WHERE '+self.argsql[key]
					params.append(value)
					first = False
		try:
			self.cur.execute(sql+";", params)
		except psycopg2.Error, e:
			self.conn.rollback()
			return e.pgerror

		data = self.cur.fetchall()
		toreturn = []
		for field in data:
			toreturn.append({
				'id':field[0],
				'player': field[1],
				'online': field[2],
				'time': str(field[3]),
				})
		return toreturn

	@classmethod
	def handle_object(self, eventid, args):
		sql = 'SELECT * FROM skynet_events WHERE id=%s;'
		self.cur.execute(sql, (eventid,))
		data = self.cur.fetchall()[0]
		return {
			'id':data[0],
			'player':data[1],
			'online':data[2],
			'time': str(data[3]),
		}

@cat_handler('players')
class player_handler(base_handler):
	argsql = {
		'from': 'time>=%s',
		'until': 'time<=%s',
	}
	@classmethod
	def handle_category(self, args):
		sql = 'SELECT DISTINCT player_name from skynet_events;'
		self.cur.execute(sql)
		data = self.cur.fetchall()
		toreturn = []
		for field in data:
			toreturn.append(field[0])
		return toreturn

	@classmethod
	def handle_object(self, player, args):
		sql = 'SELECT * FROM skynet_events WHERE LOWER(player_name)=LOWER(%s)'
		params = [player,]
		for key, value in args.iteritems():
			if key in self.argsql:
				sql+=' AND '+self.argsql[key]
				params.append(value)
		try:
			self.cur.execute(sql+"ORDER BY time ASC;", params)
		except psycopg2.Error, e:
			self.conn.rollback()
			return e.pgerror
		
		data = self.cur.fetchall()
		toreturn = []
		length = len(data)-1
		for index, field in enumerate(data):
			if field[2]:
				index+=1
				while index<=length and data[index][2]:
					data.pop(index)
					length-=1
				time = (data[index][3] - field[3]) if index<=length else (datetime.datetime.now(pytz.utc)-field[3])
				toreturn.append({
					'login_time': str(field[3]),
					'online_time': str(time),
				})
		return toreturn

@cat_handler('times')
class time_handler(base_handler):
	@classmethod
	def handle_category(self, args):
		params = []
		if 'at' in args:
			sql = """SELECT se1.player_name, se1.time, se1.online FROM skynet_events se1 
				INNER JOIN (
					SELECT MAX(se3.time) AS time, se3.player_name AS player_name FROM (
						SELECT time, player_name FROM skynet_events WHERE time<=%s) se3
					GROUP BY player_name) se2
				ON (se1.player_name = se2.player_name AND se1.time = se2.time AND se1.online = True);"""
			params.append(args['at'])
		else:
			sql = """SELECT se1.player_name, se1.time, se1.online FROM skynet_events se1 
				INNER JOIN (
					SELECT MAX(time) AS time, player_name FROM skynet_events
					GROUP BY player_name) se2
				ON (se1.player_name = se2.player_name AND se1.time = se2.time AND se1.online = True);"""
		try:
			self.cur.execute(sql, params)
		except psycopg2.Error, e:
			self.conn.rollback()
			return e.pgerror

		data = self.cur.fetchall()
		toreturn = {}
		for field in data:
			toreturn[field[0]]=str(field[1])
		return toreturn