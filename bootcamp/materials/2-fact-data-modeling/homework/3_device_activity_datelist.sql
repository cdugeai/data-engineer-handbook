WITH
-- Dedupe events
int_deduped_events AS (
    SELECT
        *,
        row_number()
            OVER (PARTITION BY url, device_id, user_id, event_time)
        AS rownum
    FROM events
),

deduped_events AS (

    SELECT *
    FROM int_deduped_events
    WHERE rownum = 1
),

-- Dedupe devices
int_deduped_devices AS (
    SELECT
        *,
        row_number()
            OVER (PARTITION BY device_id)
        AS rownum
    FROM devices
),

deduped_devices AS (

    SELECT *
    FROM int_deduped_devices
    WHERE rownum = 1
),

t_events AS (
    SELECT
        user_id,
        device_id,
        event_time::timestamp AS event_time
    FROM deduped_events
    WHERE user_id IS NOT null
    ORDER BY user_id
),

t_devices AS (
    SELECT
        device_id,
        browser_type
    FROM deduped_devices
    --where browser_type is not null
),

events_with_browser_type AS (

    SELECT
        e.event_time,
        e.device_id,
        d.browser_type,
        e.user_id
    FROM t_events AS e
    LEFT JOIN t_devices AS d ON e.device_id = d.device_id
    where browser_type is not null
),

yesterday AS (
    SELECT * FROM user_devices_cumulated
    WHERE current_day = '2023-01-09'::date
),

today AS (
    SELECT
        user_id,
        browser_type,
        array_agg(event_time) AS device_activity_datelist,
        '2023-01-10'::date AS current_day
    FROM events_with_browser_type
    WHERE date_trunc('day', event_time) = '2023-01-10'::date
    GROUP BY user_id, browser_type
)

INSERT INTO user_devices_cumulated

SELECT
    coalesce(y.user_id, t.user_id) AS user_id,
    coalesce(y.browser_type, t.browser_type) AS browser_type,
    CASE
        WHEN y.device_activity_datelist IS null THEN t.device_activity_datelist
        WHEN t.device_activity_datelist IS null THEN y.device_activity_datelist
        ELSE array_cat(y.device_activity_datelist, t.device_activity_datelist)
    END AS device_activity_datelist,
    coalesce(
        t.current_day, y.current_day + interval '1 day'
    )::date AS current_day
FROM today AS t
FULL OUTER JOIN yesterday AS y
    ON t.user_id = y.user_id AND t.browser_type = y.browser_type
