CREATE TABLE host_activity_reduced (
    month date,
    host text,
    hit_array integer [],
    unique_visitors_array numeric [],
    PRIMARY KEY (month, host)
);
