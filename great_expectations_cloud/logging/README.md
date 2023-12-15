# Logging

The GX Agent offers two types of logging by default. It also allows users to pass in a logging configuration file.

## 1. Stdout logging

The default log level for output to stdout is WARN.

Users can provide an optional `--log-level` argument to specify a different logging level for stdout if desired; e.g., `--log-level=info`

These logs can be accessed using the `docker logs {container_name}` command. They are available both when a container is running and after it is exited.

## 2. Debug log files

By default, the GX Agent creates temporary log files inside of running Docker containers. These files can be provided to the Great Expectations Support team if debugging assistance is needed.

Users can provide an optional `--skip-log-file` argument to skip saving log output to files if desired.

A new log file is generated each day with a timestamped filename. The most recent 30 files are kept on a rotating basis.

These files are saved to a `logs` directory in the Docker container and are not accessible after a container has been exited.

Users can access the `logs` directory inside of a running container with the following command(s):

* To copy the logs directory to your local machine: `docker cp {dockerImageId}:/app/logs ~/Desktop/logs`
* To access the logs directory in the container: `docker exec -it {dockerImageName} /bin/bash`

## 3. Custom logging configuration

Users can optionally configure the root logger for the application by using the `--log_cfg_file` argument with the path of the log configuration file. If a file is provided, other arguments are ignored.

See the documentation for the logging.config.dictConfig method for details. https://docs.python.org/3/library/logging.config.html#logging-config-dictschema
