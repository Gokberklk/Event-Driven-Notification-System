-- W3C traceparent of the request that created the notification, so the
-- worker can continue the same distributed trace after the queue hop.
ALTER TABLE notifications ADD COLUMN trace_parent TEXT;
