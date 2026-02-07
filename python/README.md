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

## Development

This project uses a Rust extension for performance, integrated with Python using `maturin`. All development, building, and testing should be performed within a Docker environment to ensure consistent results and avoid local environment conflicts.

### Prerequisites

*   Docker installed and running.

### Build and Test Workflow

1.  **Build the Docker Development Image:**
    Navigate to the root of the project and build the Docker image for the Python directory. This image includes Rust, Python, `maturin`, and `pytest`.

    ```bash
    docker build -t python-mmap-dev python/
    ```

2.  **Run Tests:**
    Execute the tests within the Docker container. This command will first build the Rust extension using `maturin build`, then install the generated Python wheel, and finally run `pytest`.

    ```bash
    docker run --rm -v "$(pwd)/python:/app" -w /app python-mmap-dev bash -c "maturin build && pip install target/wheels/*.whl --force-reinstall && python -m pytest"
    ```
    *Note: `$(pwd)` will correctly resolve to your current project root directory on Linux/macOS. For Windows, you might need to adjust the volume mount path, e.g., `-v "C:\Users\YourUser\path\to\otlp-mmap\python:/app"` if running directly in cmd/PowerShell, or use MSYS-compatible tools like Git Bash.*

3.  **Build Python Wheels (for distribution):**
    To build distributable Python wheels, run:

    ```bash
    docker run --rm -v "$(pwd)/python:/app" -w /app python-mmap-dev maturin build --release
    ```
    The generated wheel files will be located in `python/target/wheels/` on your host machine.

