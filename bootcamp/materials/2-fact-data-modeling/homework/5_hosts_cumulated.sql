DROP TABLE IF EXISTS hosts_cumulated;

CREATE TABLE hosts_cumulated (
    host text,
    host_activity_datelist date [],
    current_day date,
    PRIMARY KEY (host, current_day)
);
