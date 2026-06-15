-- 01a_add_event_time.sql
-- Add a computed event-time column to the source table.
--
-- The payload carries `ts` as epoch milliseconds (a plain BIGINT) -- we do NOT rely on an Avro
-- logical type being preserved end-to-end. Convert it to an event-time TIMESTAMP_LTZ here; the
-- watermark is declared on this column in the next statement (Confluent Flink accepts only one
-- statement at a time, so the two ALTERs are submitted separately).
ALTER TABLE `gpu_telemetry`
  ADD `event_time` AS TO_TIMESTAMP_LTZ(`ts`, 3);
