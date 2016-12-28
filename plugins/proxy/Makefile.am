plugindir = $(libdir)/mysql-proxy/plugins

plugin_LTLIBRARIES = libproxy.la
libproxy_la_LDFLAGS  = -export-dynamic -no-undefined -avoid-version -dynamic
libproxy_la_SOURCES  = proxy-plugin.c proxy-percentile.c proxy-sql-log.c
libproxy_la_LIBADD   = $(EVENT_LIBS) $(GLIB_LIBS) $(GMODULE_LIBS) $(MYSQL_LIBS) $(top_builddir)/src/libmysql-proxy.la $(top_builddir)/src/libsql-tokenizer.la
libproxy_la_CPPFLAGS = $(MYSQL_CFLAGS) $(GLIB_CFLAGS) $(LUA_CFLAGS) $(GMODULE_CFLAGS) -I$(top_srcdir)/src/ -I$(top_srcdir)/lib/
noinst_HEADERS = proxy-plugin.h
