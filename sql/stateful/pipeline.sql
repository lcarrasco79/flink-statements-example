INSERT INTO `flink-demo-stateful-output`
(
    join_key,
    left_event_id,
    right_event_id,
    left_payload,
    right_payload,
    joined_at
)
SELECT
    l.join_key,
    l.event_id,
    r.event_id,
    l.payload,
    r.payload,
    CURRENT_TIMESTAMP
FROM `flink-demo-left` AS l
JOIN `flink-demo-right` AS r
  ON l.join_key = r.join_key
 AND r.event_ts BETWEEN l.event_ts - INTERVAL '5' MINUTE
                    AND l.event_ts + INTERVAL '5' MINUTE;
