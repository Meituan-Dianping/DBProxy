plugindir = $(libdir)/mysql-proxy/plugins

plugin_LTLIBRARIES = libadmin.la
libadmin_la_LDFLAGS  = -export-dynamic -no-undefined -avoid-version -dynamic
libadmin_la_SOURCES  = admin-plugin.c
libadmin_la_LIBADD   = $(EVENT_LIBS) $(GLIB_LIBS) $(GMODULE_LIBS) $(top_builddir)/src/libmysql-proxy.la $(top_builddir)/src/libsql-tokenizer.la
libadmin_la_CPPFLAGS = $(MYSQL_CFLAGS) $(GLIB_CFLAGS) $(LUA_CFLAGS) $(GMODULE_CFLAGS) -I$(top_srcdir)/src/


