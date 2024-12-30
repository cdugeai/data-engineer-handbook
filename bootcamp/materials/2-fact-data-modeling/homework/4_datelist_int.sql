WITH today AS (
    SELECT * FROM user_devices_cumulated
    WHERE
        current_day = '2023-01-10'::date
),

date_series_10_days AS (
    SELECT
        date_series::date AS date_series,
        date_part('day', '2023-01-10'::date - date_series) AS days_from_today
    FROM
        generate_series(
            '2023-01-01'::date, '2023-01-10'::date, interval '1 day'
        ) AS date_series
),

get_if_date_is_in_activity_list AS (
    SELECT
        *,
        coalesce(
            array_position(device_activity_datelist, date_series) > 0, false
        ) AS is_date_series_in_activity_list
    FROM today
    CROSS JOIN date_series_10_days
),

get_datelist_int_element AS (
    SELECT
        *,
        (CASE
            WHEN
                is_date_series_in_activity_list
                THEN pow(2, 10 - days_from_today - 1)::int
            ELSE 0::int
        END)::bit(10) AS datelist_int_element
    FROM get_if_date_is_in_activity_list
),

get_datelist_int AS (
    SELECT
        user_id,
        browser_type,
        device_activity_datelist,
        sum(datelist_int_element::int)::bit(10) AS datelist_int
    FROM get_datelist_int_element
    GROUP BY user_id, browser_type, device_activity_datelist
)


SELECT
    *,
    bit_count(datelist_int) AS active_days
FROM get_datelist_int
