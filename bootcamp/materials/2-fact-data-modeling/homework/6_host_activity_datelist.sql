WITH t_events_today AS (
    SELECT
        host,
        event_time,
        event_time::date AS event_day,
        row_number() OVER (PARTITION BY host, event_time::date) AS rrank
    FROM events
    WHERE event_time::date = '2023-01-06'::date
),

t_events_dedupe_day AS (
    SELECT
        host,
        event_day
    FROM t_events_today
    WHERE rrank = 1
),


yesterday AS (
    SELECT * FROM hosts_cumulated
    WHERE current_day = ('2023-01-06'::date - interval '1 day')::date
),

today AS (
    SELECT
        host,
        array_agg(event_day) AS host_activity_datelist,
        '2023-01-06'::date AS current_day
    FROM t_events_dedupe_day
    GROUP BY host
),


merged AS (
    SELECT
        coalesce(t.host, y.host) AS host,
        CASE
            WHEN t.host_activity_datelist IS null THEN y.host_activity_datelist
            WHEN y.host_activity_datelist IS null THEN t.host_activity_datelist
            ELSE array_cat(y.host_activity_datelist, t.host_activity_datelist)
        END AS host_activity_datelist,
        coalesce(
            t.current_day, y.current_day + interval '1 day'
        )::date AS current_day
    FROM today AS t FULL OUTER JOIN yesterday AS y
        ON t.host = y.host
)

INSERT INTO hosts_cumulated
SELECT * FROM merged
