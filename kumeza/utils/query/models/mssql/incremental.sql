SELECT * FROM {{source}} WHERE {{incremental_col}} >= '{{start_time}}' AND {{incremental_col}} <= '{{end_time}}'
