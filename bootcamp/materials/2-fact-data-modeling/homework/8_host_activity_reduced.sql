WITH t_events AS (
    SELECT
        user_id,
        host,
        event_time::date AS event_day
    FROM events
    WHERE
        user_id IS NOT null
        AND date_trunc('day', event_time::date) = '2023-01-06'
    LIMIT 1000
),

metrics_day AS (
    SELECT
        host,
        event_day,
        count(*) AS hits,
        count(DISTINCT user_id) AS unique_visitors
    FROM t_events
    GROUP BY host, event_day
),



yesterday AS (
    SELECT * FROM host_activity_reduced
    WHERE month = date_trunc('month', '2023-01-06'::date - interval '1 day')
),

today AS (
    SELECT
        host,
        event_day,
        date_trunc('month', event_day)::date AS month,
        hits,
        unique_visitors
    FROM metrics_day
),

merged AS (
    SELECT
        coalesce(y.month, t.month) AS month,
        coalesce(y.host, t.host) AS host,
        CASE
            WHEN
                y.hit_array IS NOT null
                THEN array_append(y.hit_array, coalesce(t.hits, 0))
            ELSE ARRAY[t.hits]
        END AS hit_array,
        CASE
            WHEN
                y.unique_visitors_array IS NOT null
                THEN
                    array_append(
                        y.unique_visitors_array, coalesce(t.unique_visitors, 0)
                    )
            ELSE ARRAY[t.unique_visitors]
        END AS unique_visitors_array
    FROM today AS t
    FULL OUTER JOIN yesterday AS y
        ON t.month = y.month AND t.host = y.host

)


INSERT INTO host_activity_reduced
SELECT * FROM merged

ON CONFLICT (month, host)
DO
UPDATE SET hit_array = excluded.hit_array,
unique_visitors_array = excluded.unique_visitors_array;
