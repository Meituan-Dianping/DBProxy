#
# Check if server has support for loading plugins
#
if (`SELECT @@have_dynamic_loading != 'YES'`) {
  --skip Example plugin requires dynamic loading
}

#
# Check if the variable SCALABILITY_METRICS is set
#
if (!$SCALABILITY_METRICS) {
  --skip Example plugin requires the environment variable \$SCALABILITY_METRICS to be set (normally done by mtr)
}

#
# Check if --plugin-dir was setup for exampledb
#
if (`SELECT CONCAT('--plugin-dir=', REPLACE(@@plugin_dir, '\\\\', '/')) != '$SCALABILITY_METRICS_OPT/'`) {
  --skip Example plugin requires that --plugin-dir is set to the example plugin dir (either the .opt file does not contain \$SCALABILITY_METRICS_OPT or another plugin is in use)
}
enable_query_log;
