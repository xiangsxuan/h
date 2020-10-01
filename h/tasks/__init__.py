"""Background worker task definitions for the h application."""

# Connection fallback set by`kombu.connection._extract_failover_opts`
# Which are used when retrying a connection as sort of documented here:
# https://kombu.readthedocs.io/en/latest/reference/kombu.connection.html#kombu.connection.Connection.ensure_connection
# Maximum number of times to retry. If this limit is exceeded the
# connection error will be re-raised.

RETRY_POLICY_QUICK = {
    "max_retries": 2,
    # The delay until the first retry
    "interval_start": 0.2,
    #  How many seconds added to the interval for each retry
    "interval_step": 0.2,
    # Maximum number of seconds to sleep between each retry
    "interval_max": 0.6,
}

RETRY_POLICY_VERY_QUICK = {
    "max_retries": 2,
    # The delay until the first retry
    "interval_start": 0,
    #  How many seconds added to the interval for each retry
    "interval_step": 0.1,
    # Maximum number of seconds to sleep between each retry
    "interval_max": 0.3,
}
