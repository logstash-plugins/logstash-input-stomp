## 3.0.8
  - Docs: Set the default_codec doc attribute.

## 3.0.7
  - Update gemspec summary

## 3.0.6
  - Wait till pipeline is in run state to connect. Before this patch this plugin
    could block logstash indefinitely due to a connect method that never finishes.

## 3.0.5
  - Fix some documentation issues

# 3.0.3
 - Docs: Add plugin description

# 3.0.2
 - Added support for reconnecting when the broker is not available

## 3.0.1
  - Relax constraint on logstash-core-plugin-api to >= 1.60 <= 2.99

# 3.0.0 (2016-05-20)
  - Breaking: Updated to use new Java APIs

# 2.0.5
  - Depend on logstash-core-plugin-api instead of logstash-core, removing the need to mass update plugins on major releases of logstash

# 2.0.4
  - New dependency requirements for logstash-core for the 5.0 release

## 2.0.3
 - Bugfix https://github.com/logstash-plugins/logstash-input-stomp/issues/10

## 2.0.0
 - Plugins were updated to follow the new shutdown semantic, this mainly allows Logstash to instruct input plugins to terminate gracefully,
   instead of using Thread.raise on the plugins' threads. Ref: https://github.com/elastic/logstash/pull/3895
 - Dependency on logstash-core update to 2.0
