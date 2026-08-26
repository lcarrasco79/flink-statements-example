INSERT INTO `flink-demo-output`
(
    event_id,
    event_type,
    payload,
    event_ts
)
SELECT
    event_id,
    event_type,
    payload,
    event_ts
FROM `flink-demo-input`;
