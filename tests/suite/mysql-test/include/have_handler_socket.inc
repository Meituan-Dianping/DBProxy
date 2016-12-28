if (`SELECT @@have_dynamic_loading != 'YES'`) {
  --skip HandlerSocket plugin requires dynamic loading
}

if (!$HANDLER_SOCKET) {
  --skip HandlerSocket plugin requires the environment variable $HANDLER_SOCKET to be set (normally done by MTR)
}

#
# Check if --plugin-dir was setup for HandlerSocket
#
if (`SELECT CONCAT('--plugin-dir=', REPLACE(@@plugin_dir, '\\\\', '/')) != '$HANDLER_SOCKET_OPT/'`) {
  --skip HandlerSocket plugin requires that --plugin-dir is set to the plugin dir (either the .opt file does not contain $HANDLER_SOCKET_OPT or another plugin is in use)
}
enable_query_log;
