-- 01b_set_watermark.sql
-- Declare the event-time watermark on the computed `event_time` column (added in 01a).
-- A 5s out-of-orderness bound is generous for the synthetic stream.
ALTER TABLE `gpu_telemetry`
  MODIFY WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND;
