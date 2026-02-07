# Python Example

This implements a Python OpenTelemetry SDK by passing important
methods into rust.

## Architecture

We expose the following low-level functions in Rust for Python:

- `record_string`: Return an integer refernce. Takes a string as input.
- `create_resource`: Returns an integer reference. Has the following arguments:
    - attributes: A dictionary of string-value pairs.
    - schema_url: An (optional) string.    
- `create_instrumentation_scope`: Returns an integer reference. Has the following arguments:
  - resource_ref: An integer (from the `create_resource` method)
  - name: A string
  - version: An (optional) string.
  - attributs: A dictionary of string-value pairs.
- `create_metric_stream`: Returns an integer reference. Takes the following arguments:
  - insturmentation_scope_ref: An integer (from the `create_instrumentation_scope` method)
  - name: A string
  - description: A string
  - unit: A string
  - aggregation: A dictionary / enum that could be one of the following:
    - `gauge`
    - `sum`, which alos contains the following attributes:
      - `aggregation_temporality`: An enum of DELTA or CUMULATIVE.
      - `is_monotonic`: A boolean
    - `histogram`, which also contains the following attributes:
      - `aggregation_temporality`: An enum of DELTA or CUMULATIVE.
      - `bucket_boundaries`: A list of doubles
    - `exp_histogram`, which also contains the following attributes:
      - `aggregation_temporality`: An enum of DELTA or CUMULATIVE.
      - `max_buckets`: An integer
      - `max_scale`: An integer
- `record_measurement`: Returns nothing, has the following arguments:
  - `metric_ref`: An integer (returned from `create_metric_stream`).
  - `attributes`: A dictionary of string-any values.
  - `time_unix_nano`: An integer
  - `value`: A double
  - `span_context`: A dictionary
- `record_event`: Returns nothing, has the following arguments:
  - `insturmentation_scope_ref`: An integer (from the `create_instrumentation_scope` method)
  - `span_context`: A dictionary
  - `event_name_ref`: An integer
  - `time_unix_nano`: A 64-bit integer
  - `attributes`: A dictionary
- `record_span_start`: Returns nothing, has the following arguments:
  - `insturmentation_scope_ref`: An integer (from the `create_instrumentation_scope` method)
  - `trace_id`: A 16-byte array
  - `span_id`: An 8-byte array
  - `parent_span_id`: An 8-byte array
  - `flags`: A 32-bit integer
  - `name`: A string
  - `kind`: An enum
  - `start_time_unix_nano`: A 64-bit integer
  - `attributes`: A dictionary of string-any values.
- `record_span_end`: Returns nothing, has the following arguments:
  - `insturmentation_scope_ref`: An integer (from the `create_instrumentation_scope` method)
  - `trace_id`: A 16-byte array
  - `span_id`: An 8-byte array
  - `end_time_unix_nano`: A 64-bit integer
