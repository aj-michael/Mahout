Summary: Ganglia Web Frontend
Name: ganglia-web
Version: 3.5.7
URL: http://ganglia.info
Release: 1
License: BSD
Vendor: Ganglia Development Team <ganglia-developers@lists.sourceforge.net>
Group: System Environment/Base
Source: %{name}-%{version}.tar.gz
Buildroot: %{_tmppath}/%{name}-%{version}-buildroot
Obsoletes: ganglia-webfrontend
Requires: php >= 5, php-gd
%if 0%{?suse_version}
%define web_prefixdir /srv/www/htdocs/ganglia
%else
%define web_prefixdir %{custom_web_prefixdir}
%endif

%{!?custom_web_prefixdir: %define web_prefixdir /var/www/html/ganglia}

Prefix: %{web_prefixdir}
BuildArchitectures: noarch

%description
This package provides a web frontend to display the XML tree published by
ganglia, and to provide historical graphs of collected metrics. This website is
written in the PHP5 language and uses the Dwoo templating engine.

%prep
%setup -n %{name}-%{version}

%build

%install
# Flush any old RPM build root
%__rm -rf $RPM_BUILD_ROOT

%__mkdir -p $RPM_BUILD_ROOT/%{web_prefixdir}
%__cp -rf * $RPM_BUILD_ROOT/%{web_prefixdir}
%__rm -rf $RPM_BUILD_ROOT/%{web_prefixdir}/conf
%__install -d -m 0755 $RPM_BUILD_ROOT/var/lib/ganglia-web/filters
%__install -d -m 0755 $RPM_BUILD_ROOT/var/lib/ganglia-web/conf
%__cp -rf conf/* $RPM_BUILD_ROOT/var/lib/ganglia-web/conf
%__install -d -m 0755 $RPM_BUILD_ROOT/var/lib/ganglia-web/dwoo
%__install -d -m 0755 $RPM_BUILD_ROOT/var/lib/ganglia-web/dwoo/compiled
%__install -d -m 0755 $RPM_BUILD_ROOT/var/lib/ganglia-web/dwoo/cache

%files
%defattr(-,root,root)
%attr(0755,nobody,nobody)/var/lib/ganglia-web/filters
%attr(0755,apache,apache)/var/lib/ganglia-web/conf
%attr(0755,apache,apache)/var/lib/ganglia-web/dwoo
%attr(0755,apache,apache)/var/lib/ganglia-web/dwoo/compiled
%attr(0755,apache,apache)/var/lib/ganglia-web/dwoo/cache
%{web_prefixdir}/*
/var/lib/ganglia-web/conf/*
%config(noreplace) %{web_prefixdir}/conf_default.php

%clean
%__rm -rf $RPM_BUILD_ROOT

%changelog
* Thu Mar 17 2011 Bernard Li <bernard@vanhpc.org>
- Renamed conf.php -> conf_default.php
* Fri Dec 17 2010 Bernard Li <bernard@vanhpc.org>
- Spec file for gweb which is split from ganglia-web subpackage
