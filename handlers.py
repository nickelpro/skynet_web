#Depends on web.py, psycopg2, PyYaml, pytz, python-dateutil
import web, psycopg2
import json, yaml, xmlrpclib
import datetime, pytz
import skysql
from dateutil.parser import parse as datetimeparse
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
	'yml': 'text/x-yaml',
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
			web.header('Content-Type', ctypes[returntype])
			return returntypes[returntype](categories[category].handle_object(obj, args))
		else:
			return web.NotFound()

class base_handler:
	conn = psycopg2.connect(database=dbname, user=dbuser, password=dbpass)
	@classmethod
	def handle_category(self, args):
		return web.NotFound()
	@classmethod
	def handle_object(self, obj, args):
		return web.NotFound()

@cat_handler('events')
class events_handler(base_handler):
	argsql = {
		'from': 'time>=%s',
		'until': 'time<=%s',
		'player': 'LOWER(player_name)=LOWER(%s)',
		'online': 'online=%s',
	}
	@classmethod
	def handle_category(self, args):
		cur = self.conn.cursor()
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
			cur.execute(sql+";", params)
		except psycopg2.Error, e:
			cur.close()
			self.conn.rollback()
			return e.pgerror

		data = cur.fetchall()
		cur.close()
		toreturn = []
		for field in data:
			toreturn.append({
				'id':field[0],
				'player': field[1],
				'online': field[2],
				'time': field[3].isoformat(),
				})
		return toreturn

	@classmethod
	def handle_object(self, eventid, args):
		cur = self.conn.cursor()
		sql = 'SELECT * FROM skynet_events WHERE id=%s;'
		try:
			cur.execute(sql, (eventid,))
		except psycopg2.Error, e:
			cur.close()
			self.conn.rollback()
			return e.pgerror

		data = cur.fetchall()[0]
		cur.close()
		return {
			'id':data[0],
			'player':data[1],
			'online':data[2],
			'time': data[3].isoformat(),
		}

@cat_handler('players')
class players_handler(base_handler):
	@classmethod
	def handle_category(self, args):
		cur = self.conn.cursor()
		sql = 'SELECT DISTINCT player_name FROM skynet_events;'
		try:
			cur.execute(sql)
		except psycopg2.Error, e:
			cur.close()
			self.conn.rollback()
			return e.pgerror

		data = cur.fetchall()
		cur.close()
		toreturn = []
		for field in data:
			toreturn.append(field[0])
		return toreturn

	@classmethod
	def handle_object(self, player, args):
		cur = self.conn.cursor()
		sql = 'SELECT * FROM skynet_events WHERE LOWER(player_name)=LOWER(%s) ORDER BY time ASC;'
		try:
			cur.execute(sql, (player,))
		except psycopg2.Error, e:
			cur.close()
			self.conn.rollback()
			return e.pgerror

		data = cur.fetchall()
		cur.close()
		toreturn = []
		#I'm sure this could be done in SQL but this way is simpler
		length = len(data)-1
		for index, field in enumerate(data):
			if field[2]:
				index+=1
				while index<=length and data[index][2]:
					data.pop(index)
					length-=1
				time = (data[index][3] - field[3]) if index<=length else (datetime.datetime.now(pytz.utc)-field[3])
				try:
					if not 'until' in args or datetimeparse(args['until'])>=field[3]:
						if not 'from' in args or datetimeparse(args['from'])<=field[3]:
							toreturn.append({
								'login_time': field[3].isoformat(),
								'online_time': str(time),
							})
				except ValueError, e:
					return [str(e),]
		return toreturn

#Similar to players, but provides total session information
@cat_handler('times')
class times_handler(base_handler):
	@classmethod
	def handle_category(self, args):
		cur = self.conn.cursor()
		sql = 'SELECT DISTINCT player_name FROM skynet_events;'
		try:
			cur.execute(sql)
		except psycopg2.Error, e:
			cur.close()
			self.conn.rollback()
			return e.pgerror

		data = cur.fetchall()
		cur.close()
		toreturn = []
		for field in data:
			toreturn.append(field[0])
		return toreturn

	@classmethod
	def handle_object(self, player, args):
		cur = self.conn.cursor()
		sql = 'SELECT * FROM skynet_events WHERE LOWER(player_name)=LOWER(%s) ORDER BY time ASC;'
		try:
			cur.execute(sql, (player,))
		except psycopg2.Error, e:
			cur.close()
			self.conn.rollback()
			return e.pgerror

		data = cur.fetchall()
		cur.close()
		toreturn = datetime.timedelta()
		#I'm sure this could be done in SQL but this way is simpler
		length = len(data)-1
		for index, field in enumerate(data):
			if field[2]:
				index+=1
				while index<=length and data[index][2]:
					data.pop(index)
					length-=1
				time = (data[index][3] - field[3]) if index<=length else (datetime.datetime.now(pytz.utc)-field[3])
				try:
					if not 'until' in args or datetimeparse(args['until'])>=field[3]:
						if not 'from' in args or datetimeparse(args['from'])<=field[3]:
							toreturn += time
				except ValueError, e:
					return str(e)
		return str(toreturn)


@cat_handler('online')
class online_handler(base_handler):
	argsql = {
		'from': 'time>=%s',
		'until': 'time<=%s',
	}
	@classmethod
	def handle_category(self, args):
		cur = self.conn.cursor()
		params = []
		if 'at' in args:
			sql = skysql.online_at
			params.append(args['at'])
		elif ('from' in args) or ('until' in args):
			sql = 'SELECT DISTINCT player_name, time FROM skynet_events'
		 	first = True
		 	for key, value in args.iteritems():
		 		if key in self.argsql:
		 			if not first:
		 				sql+=' AND '+self.argsql[key]
		 				params.append(value)
		 			else:
		 				sql+=' WHERE '+self.argsql[key]
		 				params.append(value)
		 				first = False
		 	
		else:
			sql = skysql.online_now
		try:
			cur.execute(sql, params)
		except psycopg2.Error, e:
			cur.close()
			self.conn.rollback()
			return e.pgerror

		data = cur.fetchall()
		cur.close()
		toreturn = {}
		for field in data:
			toreturn[field[0]]=field[1].isoformat()
		return toreturn
