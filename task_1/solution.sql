WITH sessions_daterange AS (
    SELECT
        us_open.id_session,
        us_open.id_user,
        us_open.date_action AS dt_open,
        us_close.date_action AS dt_close
    FROM users_sessions AS us_open
    FULL JOIN users_sessions AS us_close
        ON us_open.id_session = us_close.id_session
        AND us_open.action = 'open'
        AND us_close.action = 'close'
    WHERE us_open.date_action >= NOW()::DATE - INTERVAL '10 days'
),
splited_sessions_dateranges AS (
    SELECT 
        id_session,
        id_user,
        CASE WHEN dt_open::DATE = d THEN dt_open ELSE d END AS start_dt,
        CASE WHEN dt_close::DATE = d THEN dt_close ELSE d + INTERVAL '1 day' END AS end_dt
    FROM (
        SELECT 
            id_session,
            id_user,
            dt_open,
            dt_close,
            GENERATE_SERIES(dt_open::DATE, dt_close::DATE, INTERVAL '1 day')::DATE AS d
        FROM sessions_daterange sdt
    ) subq
)
SELECT
    id_user,
    start_dt::DATE,
    SUM(
        EXTRACT(EPOCH FROM (end_dt - start_dt)) / 3600
    ) AS duration_by_day
FROM splited_sessions_dateranges
GROUP BY id_user, start_dt::DATE
ORDER BY start_dt::DATE DESC, duration_by_day DESC;
