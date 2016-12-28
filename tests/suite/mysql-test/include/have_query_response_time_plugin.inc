#
# Check if server has support for loading plugins
#
if (`SELECT @@have_dynamic_loading != 'YES'`) {
  --skip query_response_time plugin requires dynamic loading
}

#
# Check if the variable PLUGIN_QUERY_RESPONSE_TIME  is set
#
if (!$PLUGIN_QUERY_RESPONSE_TIME) {
  --skip query_response_time plugin requires the environment variable $PLUGIN_QUERY_RESPONSE_TIME to be set (normally done by mtr)
}

#
# Check if --plugin-dir was setup for exampledb
#
if (`SELECT CONCAT('--plugin-dir=', REPLACE(@@plugin_dir, '\\\\', '/')) != '$PLUGIN_QUERY_RESPONSE_TIME_OPT/'`) {
  --skip query_response_time plugin requires that --plugin-dir is set to the plugin dir (either the .opt file does not contain $PLUGIN_QUERY_RESPONSE_TIME_OPT or another plugin is in use)
}
enable_query_log;
