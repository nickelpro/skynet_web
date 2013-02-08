# Stupid complex SQL because edge cases make it very hard to accurately
# retrieve the time someone logged in. This gets pretty close
online_now = """
--Find which players are online
WITH online_players AS (
	SELECT se1.player_name, se1.time, se1.online FROM skynet_events se1 INNER JOIN (
		SELECT MAX(time) AS time, player_name FROM skynet_events GROUP BY player_name
	) se2 ON (se1.player_name = se2.player_name AND se1.time = se2.time AND se1.online = True)
),
--Get the log in times based on last logout
log_in AS (
	SELECT MIN(se1.time) AS time, se1.player_name FROM skynet_events se1 INNER JOIN (
		SELECT se2.player_name AS player_name, se2.time AS time FROM (
			SELECT MAX(se3.time) AS time, se3.player_name FROM (
				SELECT time, player_name, online FROM skynet_events WHERE online = False
			) se3 GROUP BY player_name
		) se2 INNER JOIN (
			SELECT player_name FROM online_players
		) se4 ON (se2.player_name = se4.player_name)
	) se5 ON (se1.player_name = se5.player_name) WHERE se1.time>se5.time GROUP BY se1.player_name
)
--Grabs all the logins including players who have never logged out before
SELECT se1.player_name, se1.time FROM log_in se1
UNION SELECT se2.player_name, MIN(se2.time) AS time FROM skynet_events se2 INNER JOIN (
	SELECT se3.player_name FROM online_players se3
	EXCEPT SELECT se4.player_name FROM log_in se4
) se5 ON (se2.player_name = se5.player_name) GROUP BY se2.player_name;
"""

online_at = """
WITH at_time AS (
	SELECT time, player_name, online FROM skynet_events WHERE time<=%s
),
WITH online_players AS (
	SELECT se1.player_name, se1.time, se1.online FROM at_time se1 INNER JOIN (
		SELECT MAX(time) AS time, player_name FROM at_time GROUP BY player_name
	) se2 ON (se1.player_name = se2.player_name AND se1.time = se2.time AND se1.online = True)
),
--Get the log in times based on last logout
log_in AS (
	SELECT MIN(se1.time) AS time, se1.player_name FROM at_time se1 INNER JOIN (
		SELECT se2.player_name AS player_name, se2.time AS time FROM (
			SELECT MAX(se3.time) AS time, se3.player_name FROM (
				SELECT time, player_name, online FROM at_time WHERE online = False
			) se3 GROUP BY player_name
		) se2 INNER JOIN (
			SELECT player_name FROM online_players
		) se4 ON (se2.player_name = se4.player_name)
	) se5 ON (se1.player_name = se5.player_name) WHERE se1.time>se5.time GROUP BY se1.player_name
)
--Grabs all the logins including players who have never logged out before
SELECT se1.player_name, se1.time FROM log_in se1
UNION SELECT se2.player_name, MIN(se2.time) AS time FROM at_time se2 INNER JOIN (
	SELECT se3.player_name FROM online_players se3
	EXCEPT SELECT se4.player_name FROM log_in se4
) se5 ON (se2.player_name = se5.player_name) GROUP BY se2.player_name;
"""
