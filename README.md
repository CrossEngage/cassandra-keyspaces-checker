# cassandra-keyspaces-checker

Simple tool to read metrics for every keyspace and table on a Cassandra, through Jolokia, then output InfluxDB line protocol.

This tool is meant to be used with Telegraf's `inputs.exec` plugin.