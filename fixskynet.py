import psycopg2
from dblogin import dbname, dbuser, dbpass

conn = psycopg2.connect(database=dbname, user=dbuser, password=dbpass)
cur = conn.cursor()

cur.execute("""SELECT se1.player_name, se1.time, se1.online FROM skynet_events se1 INNER JOIN (
					SELECT MAX(time) AS time, player_name FROM skynet_events GROUP BY player_name
				) se2 ON (se1.player_name = se2.player_name AND se1.time = se2.time AND se1.online = True)"""
)

data = cur.fetchall()
conn.commit()
for field in data:
	print field[0]
	cur.execute("""INSERT INTO skynet_events (player_name, online, time) VALUES (%s, %s, NOW());""", 
		(field[0], False,))
	conn.commit()
cur.close()
conn.close()