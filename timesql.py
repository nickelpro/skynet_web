time_at =  """
			WITH online_players AS (
					SELECT se1.player_name, se1.time, se1.online FROM skynet_events se1 
						INNER JOIN (
							SELECT MAX(time) AS time, player_name FROM skynet_events
							GROUP BY player_name
						) se2
					ON (se1.player_name = se2.player_name AND se1.time = se2.time AND se1.online = True)
			),
				log_times AS (
					SELECT MAX(se1.time) AS time, se1.player_name FROM (
						SELECT time, player_name, online FROM skynet_events WHERE online = False) se1
					GROUP BY player_name
			),
				online_log AS (
					SELECT se1.player_name, se1.time FROM log_times se1
						INNER JOIN (
							SELECT player_name FROM online_players
						) se2
					ON (se1.player_name = se2.player_name)
			),
				log_in AS (
					SELECT se1.time, se1.player_name FROM skynet_events se1
						INNER JOIN (
							SELECT player_name, time FROM online_log
						) se2
					ON (se1.player_name = se2.player_name) WHERE se1.time>se2.time
			),
				min_time AS (
					SELECT MIN(time) AS time, player_name FROM log_in GROUP BY player_name
			),
				yaw AS (
					SELECT se1.player_name, se1.time FROM min_time se1
					UNION SELECT se2.player_name, se2.time FROM online_players se2 
			)
				SELECT MIN(time) AS time, player_name FROM yaw GROUP BY player_name;
			"""
time_now = """
			WITH at_time AS (
					SELECT time, player_name, online FROM skynet_events WHERE time<=%s
			),
				online_players AS (
					SELECT se1.player_name, se1.time, se1.online FROM at_time se1 
						INNER JOIN (
							SELECT MAX(time) AS time, player_name FROM at_time
							GROUP BY player_name
						) se2
					ON (se1.player_name = se2.player_name AND se1.time = se2.time AND se1.online = True)
			),
				log_times AS (
					SELECT MAX(se1.time) AS time, se1.player_name FROM (
						SELECT time, player_name, online FROM at_time WHERE online = False) se1
					GROUP BY player_name
			),
				online_log AS (
					SELECT se1.player_name, se1.time FROM log_times se1
						INNER JOIN (
							SELECT player_name FROM online_players
						) se2
					ON (se1.player_name = se2.player_name)
			),
				log_in AS (
					SELECT se1.time, se1.player_name FROM at_time se1
						INNER JOIN (
							SELECT player_name, time FROM online_log
						) se2
					ON (se1.player_name = se2.player_name) WHERE se1.time>se2.time
			),
				min_time AS (
					SELECT MIN(time) AS time, player_name FROM log_in GROUP BY player_name
			),
				yaw AS (
					SELECT se1.player_name, se1.time FROM min_time se1
					UNION SELECT se2.player_name, se2.time FROM online_players se2 
			)
				SELECT MIN(time) AS time, player_name FROM yaw GROUP BY player_name;
			"""