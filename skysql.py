# Stupid complex SQL because edge cases make it very hard to accurately
# retrieve the time someone logged in. This gets pretty close and I'll
# be cleaning it up over the next couple days.
online_now =  """
			--Find which players are online
			WITH online_players AS (
					SELECT se1.player_name, se1.time, se1.online FROM skynet_events se1 
						INNER JOIN (
							SELECT MAX(time) AS time, player_name FROM skynet_events
							GROUP BY player_name
						) se2
					ON (se1.player_name = se2.player_name AND se1.time = se2.time AND se1.online = True)
			),
			--Get the log in times
				log_in AS (
					SELECT se1.time, se1.player_name FROM skynet_events se1
						INNER JOIN (
							SELECT player_name, time FROM (
								SELECT se2.player_name, se2.time FROM (
									SELECT MAX(se3.time) AS time, se3.player_name FROM (
										SELECT time, player_name, online FROM skynet_events WHERE online = False) se3
									GROUP BY player_name) se2
									INNER JOIN (
										SELECT player_name FROM online_players
									) AS se4
								ON (se2.player_name = se4.player_name)
							)
						) se5
					ON (se1.player_name = se5.player_name) WHERE se1.time>se5.time
			),
			--Yet Another With clause, grabs players who have never logged before
				yaw AS (
					SELECT se1.player_name, se1.time FROM log_in se1
					UNION SELECT se2.player_name, se2.time FROM skynet_events se2
						INNER JOIN (
							SELECT se3.player_name FROM online_players se3
								EXCEPT SELECT se4.player_name FROM online_log se4) se5
					ON (se2.player_name = se5.player_name)
			)
				SELECT MIN(time) AS time, player_name FROM yaw GROUP BY player_name;
			"""

online_at = """
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
				online_log AS (
					SELECT se1.player_name, se1.time FROM (
						SELECT MAX(se3.time) AS time, se3.player_name FROM (
							SELECT time, player_name, online FROM at_time WHERE online = False) se3
						GROUP BY player_name) se1
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
				yaw AS (
					SELECT se1.player_name, se1.time FROM log_in se1
					UNION SELECT se2.player_name, se2.time FROM at_time se2
						INNER JOIN (
							SELECT se3.player_name FROM online_players se3
								EXCEPT SELECT se4.player_name FROM online_log se4) se5
					ON (se2.player_name = se5.player_name)
			)
				SELECT MIN(time) AS time, player_name FROM yaw GROUP BY player_name;
			"""
