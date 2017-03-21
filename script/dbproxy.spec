%define _prefix /usr/local/mysql-proxy
%define _libdir %{_prefix}/lib
%define _docdir %{_prefix}/share/doc
#
# Simple RPM spec file for mysql-proxy
# written by Lenz Grimmer <lenz@mysql.com>
#
Summary: A Proxy for the MySQL Client/Server protocol
Name: dbproxy 
Version: 0.1
Release: 1%{?dist}
License: GPL
Group: Applications/Networking
Source: %{name}-%{version}.tar.gz
Prefix: %{_prefix}
Buildroot: %{_tmppath}/%{name}-%{version}-%{release}-root
Packager: tne.sa.dba@meituan.com
Requires: flex jemalloc openssl glib2 >= 2.32 Percona-Server-client-55 libevent
BuildRequires: mysql-devel glib2-devel >= 2.32 libevent-devel lua-devel jemalloc-devel openssl-devel Percona-Server-devel-55

%description
MySQL Proxy is a simple program that sits between your client and MySQL
server(s) that can monitor, analyze or transform their communication. Its
flexibility allows for unlimited uses; common ones include: load balancing;
failover; query analysis; query filtering and modification; and many more.

%prep
%setup

%build
%configure CFLAGS="-O0 -s" LDFLAGS="-Wl,--rpath=/usr/lib64/" 
%{__make} %{?_smp_mflags}

%install
%{__make} DESTDIR=%{buildroot} install
if [ ! -d "%{buildroot}/%{_prefix}/conf" ]; then
  mkdir %{buildroot}/%{_prefix}/conf
fi

%clean
%{__rm} -rfv %{buildroot}

%post
/sbin/ldconfig
sed -i "s;proxydir=.*;proxydir=$RPM_INSTALL_PREFIX;" $RPM_INSTALL_PREFIX/bin/mysql-proxyd

%postun
%{__rm} -rfv "$RPM_INSTALL_PREFIX"
/sbin/ldconfig

%files
%defattr(-,root,root)
%{_prefix}/conf
%{_docdir}/*
%{_bindir}/*
%{_libdir}/*
