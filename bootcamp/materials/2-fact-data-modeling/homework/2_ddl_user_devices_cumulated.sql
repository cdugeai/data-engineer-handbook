DROP TABLE IF EXISTS user_devices_cumulated;

CREATE TABLE user_devices_cumulated (
    user_id numeric,
    browser_type text,
    device_activity_datelist date [],
    current_day date
);
