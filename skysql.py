# Stupid complex SQL because edge cases make it very hard to accurately
# retrieve the time someone logged in. This gets pretty close
online_now = """
--Find which players are online
WITH online_players AS (
	SELECT se1.player_name, se1.time, se1.online FROM skynet_events se1 
		INNER JOIN (
			SELECT MAX(time) AS time, player_name FROM skynet_events
			GROUP BY player_name
		) se2
	ON (se1.player_name = se2.player_name AND se1.time = se2.time AND se1.online = True)
),
--Get the log in times based on last logout
log_in AS (
	SELECT se1.time, se1.player_name FROM skynet_events se1
		INNER JOIN (
			SELECT se2.player_name AS player_name, se2.time AS time FROM (
				SELECT MAX(se3.time) AS time, se3.player_name FROM (
					SELECT time, player_name, online FROM skynet_events WHERE online = False) se3
				GROUP BY player_name) se2
				INNER JOIN (
					SELECT player_name FROM online_players
				) se4
			ON (se2.player_name = se4.player_name)
		) se5
	ON (se1.player_name = se5.player_name) WHERE se1.time>se5.time
)
--Grabs all the logins including players who have never logged out before
SELECT MIN(se1.time) AS time, se1.player_name FROM (
	SELECT se2.player_name as player_name, se2.time as time FROM log_in se2
	UNION SELECT se3.player_name, se3.time FROM skynet_events se3
		INNER JOIN (
			SELECT se4.player_name FROM online_players se4
				EXCEPT SELECT se5.player_name FROM log_in se5) se6
	ON (se3.player_name = se6.player_name)
) se1 GROUP BY player_name;
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
log_in AS (
	SELECT se1.time, se1.player_name FROM at_time se1
		INNER JOIN (
			SELECT se2.player_name AS player_name, se2.time AS time FROM (
				SELECT MAX(se3.time) AS time, se3.player_name FROM (
					SELECT time, player_name, online FROM at_time WHERE online = False) se3
				GROUP BY player_name) se2
				INNER JOIN (
					SELECT player_name FROM online_players
				) se4
			ON (se2.player_name = se4.player_name)
		) se5
	ON (se1.player_name = se5.player_name) WHERE se1.time>se5.time
)
SELECT MIN(se1.time) AS time, se1.player_name FROM (
	SELECT se2.player_name as player_name, se2.time as time FROM log_in se2
	UNION SELECT se3.player_name, se3.time FROM at_time se3
		INNER JOIN (
			SELECT se4.player_name FROM online_players se4
				EXCEPT SELECT se5.player_name FROM log_in se5) se6
	ON (se3.player_name = se6.player_name)
) se1 GROUP BY player_name;
"""
