# Installing Infosquito

This file contains instructions for installing Infosquito and all of its
dependencies. Before beginning, please ensure that you have the appropriate YUM
repository enabled on the host or hosts where Infosquito will be installed.
Please contact Core Software with questions about enabling the YUM repository.

## Overview

* [Install beanstalkd](#install-beanstalkd)
* [Install elasticsearch](#install-elasticsearch)
* [Install infosquito](#install-infosquito)

## Install Beanstalkd

An RPM for beanstalkd is available in iPlant's YUM repositories. This RPM
provides the standard beanstalkd installation along with an initd script that
can be used to stop and start beanstalkd.

Note that the init script uses Clavin to obtain configuration settings from
Zookeeper. Clavin will automatically be installed when beanstalkd is installed
using this RPM package.

### Installation

```
$ sudo yum install iplant-beanstalk
```

### Configuration

The only configrable setting for this beanstalkd installation is the port number
that beanstalkd listens to for incoming connections.

```
beanstalk.app.listen-port = 11300
```

## Install Elasticsearch

An RPM for elasticsearch is available in iPlant's YUM repositories. This RPM
provides the standard elasticsearch installation along with an initd script that
can be used to start and stop elasticsearch.

Note that the init script uses Clavin to obtain configuration settings from
Zookeeper. Clavin will automatically be installed when elasticsearch is
installed using this RPM package.

### Installation

```
$ sudo yum install elasticsearch
```

### Configuration

The only configurable setting for this elasticsearch installation is the port
number that elasticsearch listens to for incoming connections.

```
elasticsearch.app.listen-port = 9200
```

## Install infosquito

Infosquito is also packaged as an RPM, so installation is similar to the
installation of beanstalkd and elasticsearch.

### Installation

```
$ sudo yum install infosquito
```

### Configulon

```
# Beanstalk settings
infosquito.beanstalk.host = somehost.example.org
infosquito.beanstalk.port = 11300
infosquito.beanstalk.tube = infosquito
infosquito.job-ttr        = 120
infosquito.work-tube      = infosquito

# Elasticsearch settings
infosquito.es.host                = somehost.example.org
infosquito.es.port                = 9200
infosquito.es.scroll-page-size    = 50
infosquito.es.scroll-ttl          = 10m

# iRODS settings
infosquito.irods.default-resource =
infosquito.irods.home             = /somezone/home
infosquito.irods.host             = somehost.example.org
infosquito.irods.index-root       = /somezone/home
infosquito.irods.password         = somepassword
infosquito.irods.port             = 1247
infosquito.irods.user             = rodsadmin
infosquito.irods.zone             = someotherpassword

# Miscellaneous settings
infosquito.num-instances = 2
infosquito.retry-delay   = 10000
```
