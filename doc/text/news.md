# News

## 1.1.1: 2015-11-04

 * General:
   * `search` command: The `offset` of `sortBy` parameter works correctly even if there is only one slice.
 * Groonga compatibility:
   * `select` command: The `offset` parameter works correctly with the `sortby` parameter, even if there is only one slice.

## 1.1.0: 2015-04-29

 * General:
   * Graceful stopping/restarting works correctly with continual inpouring messages.
   * A new parameter `single_operation` is introduced to single step definitions.
     If you set it to `true`, messages for the handler will be delivered to just single volume (one of replicas and slices).
     It is useful for commands which has to be executed only once in a cluster, like `system.status`.
   * A new parameter `use_all_replicas` is introduced to single step definitions.
     If you set it to `true`, messages for the handler will be delivered to all replica volumes always.
     It is useful for commands which has to be executed all replicas, like `system.statistics.object.count.per-volume`.
   * `add` command now accepts requests with automatically-convertible mismatched type keys.
     For example, a string key `"1"` is available for a table with the key type `UInt32`.
   * `dump` command: the value of a column referring any record of another table are correctly exported as its key string, instead of object value (it's invalid value for a message of the `add` command).
     As the result, now tables with reference columns are correctly copied between multiple clusters.
   * `dump` command: records in a table which has only one column `_key` are exported correctly.
   * `dump` command: forwarded messages are now have their own `date` field.
   * `Collectors::RecursiveSum` is introduced.
   * `system.status` command: now the reporter node and its internal name is reported as a part of response body.
   * `system.statistics.object.count` command is now available. It is used by command line utilities internally.
   * `system.statistics.object.count.per-volume` command is now available. It is useful to confirm equivalence of replicas.
   * `system.absorb-data` command is now available. It is used by command line utilities internally.
 * Message format:
   * `targetRole` field is introduced to the envelope.
     Now you can specify the role of the engine node which can process the messsage.
     If mismatched role node receives the message, it will be bounced to suitable node automatically if possible.
   * `timeout` field is introduced to the envelope.
     Now you can specify time to expire the request, in seconds.
 * Command line utilities:
   * `droonga-engine-join` and `droonga-engine-absorb-data` commands work more surely.
   * A new option `--verbose` is introduced to monitor internal Serf operations, for `droonga-engine-join` and `droonga-engine-absorb-data`.
   * A new command line utility `droonga-engine-set-role` is available (mainly for debugging).
 * Compatibility with Groonga:
   * Better compatibility to Groonga's `delete` command:
     * Works correctly for tables with integer key types.
       [The report by funa1g](http://sourceforge.jp/projects/groonga/lists/archive/dev/2014-December/002995.html) inspired this improvement. Thanks!
     * Accepts requests with automatically-convertible mismatched type keys.
       For example, a numeric key `1` is available for a table with the key type `ShortText`.

## 1.0.9: 2014-12-01

 * `droonga-engine-join`, `droonga-engine-unjoin`, and `droonga-engine-absorb-data` work on any host.
   Instead, you have to specify the host name or the IP address of the working host via the `--receiver-host` option.
 * Clusters are managed with their own unique id.
   In previous version, a node unjoined from the Droonga cluster is still a member of the Serf cluster, and there is no information that protocol adapter nodes detect which is actual member or not.

## 1.0.8: 2014-11-29

 * Better compatibility to Groonga's `select` command:
   * Whitespace-separeted `output_columns` (it is valid on `command_version=1` environments) is now available.
   * `output_columns=*` works correctly even if it is a `TABLE_NO_KEY` table.
 * Better compatibility to Groonga's `column_list` command:
   * A `_key` virtual column is correctly appear in the result, for tables with one of flags: `TABLE_HASH_KEY`, `TABLE_PAT_KEY`, and `TABLE_DAT_KEY`.
   * At an index column, its `source` is now compatible to Groonga's one.
 * Groonga's `table_create` command now requires `key_type` parameter for tables with one of flags: `TABLE_HASH_KEY`, `TABLE_PAT_KEY`, or `TABLE_DAT_KEY`.
   You'll get an error response, if the parameter is not given.
   (Groonga unexpectedly accepts `table_create` requests without `key_type`, but it is a bad behavior (too lazy) and it should not be covered as a compatibility.)
 * The `daemon` option is now ignored in the static configuration file.
   Now, you always have to specify `--daemon` option for the `droonga-engine` command
   to start it as a daemon.
 * The `droonga-engine-configure` command now shows prompts for all options always.
 * The rate of absorbed records are limited to 100 records per second by default, for `droonga-engine-absorb-data` and `droonga-engine-join` commands.
 * `droonga-engine-absorb-data` and `droonga-engine-join` commands now report their progress, if possible.

## 1.0.7: 2014-10-07

 * Better compatibility to Groonga: `select` command now supports `query_flags` option.
   Note: `ALLOW_UPDATE` is ignored even if you specify, because it is not implemented in Droonga yet.
 * `saerch` command has some improvements.
   * The value `false` for `allowPragma` and `allowColumn` options in query syntax search conditions is correctly applied.
     In old versions, they options are always `true` even if you intentionally specified `false` for them.
   * `allowLeadingNot` option is available in query syntax search conditions.
     It is `false` by default.
 * Works correctly as a service even if you restarted the computer itself.
 * `droonga-engine-configure` now asks the log level.

## 1.0.6: 2014-09-29

 * The installation script is now available.
   It automatically installs required softwares and configure the `droonga-engine` as a system service.
   Currently it works only for Debian, Ubuntu, and CentOS 7.
 * The service works as a process belonging to a user `droonga-engine` who is specific for the service.
   The configuration directory for the service is placed under the home directory of the user.
 * A static configuration file to define default parameters (`host` and so on) is now available.
   It must be placed into the directory same to `catalog.json`.
   You don't have to run `droonga-engine` command with many options, anymore.
 * `droonga-engine-join` now automatically fetches `catalog.json` from the specified source replica node.
   Now you don't have to copy `catalog.json` from another node before you run `droonga-engine-join` anymore.
 * A new `catalog` plugin is introduced as one of default plugins, to fetch `catalog.json` from existing cluster.
   The list of plugins in your `catalog.json` must include it.
 * A new command line utility `droonga-engine-configure` is available.
   It generates the static configuration file, the `catalog.json` for the service.
   Moreover, it clears old stored data to make the node empty.
 * Some options for utility commands become optional.
   Important parameters are automatically detected.
 * Restarts server processes more gracefully.

## 1.0.5: 2014-07-29

 * Restarts server processes more gracefully.
 * Works with search results with vector reference column values correctly.
 * Messages forwarded to other nodes are always buffered for now.
 * Works again for the case: `nWorkers` == `0`
 * droonga-engine-join: Works correctly and safely for databases with much records.

## 1.0.4: 2014-06-29

 * New command (and plugin) [`status`](http://droonga.org/reference/1.0.4/commands/status/) is now available.
 * New command line tools are available.
   * `droonga-engine-join` and `droonga-engine-unjoin` help you to modify cluster composition. See [the tutorial to add/remove replica](http://droonga.org/tutorial/1.0.4/add-replica/).
   * `droonga-engine-absorb-data` helps you to duplicate clusters. See [the tutorial for dump/restore](http://droonga.org/tutorial/1.0.4/dump-restore/).
   * `droonga-engine-catalog-modify` helps you to modify existing `catalog.json`.

## 1.0.3: 2014-05-29

 * Alive monitoring (based on [Serf](http://serfdom.io/)) lands.
   Now, nodes in a cluster observe each other, and the cluster keeps working, even if one of replicas is dead.
 * New commands to dump whole contents of an existing cluster are available.
   They are used by [`drndump`](https://github.com/droonga/drndump) internally.
 * The command line tool `droonga-catalog-generate` is renamed to `droonga-engine-catalog-generate`.
 * Use `Default` as the name of the default dataset for a `catalog.json`, generated by `droonga-catalog-generate` .
 * The path of the configuration directory is now specified via an environment variable `DROONGA_BASE_DIR`.
 * Fix incompatibilities of the `select` Groonga command.
   * The default value of the `drilldown_output_columns` option becomes same to Groonga's one.
   * Column values of `Time` type clumns are returned as float numbers correctly.
   * The message structure of results becomes same to Groonga.
     In previous version, records are wrongly wrapped in an array.
 * Improve features of the `select` command..
   * The request parameter `"attributes"` for `"elements"` in `"output"` is now available.
   * The special value `"*"` for `"attributes"` in `"output"` is now available, to export all columns.
 * Server process does shutdown/restart gracefully.
 * Restart itself automatically when the `catalog.json` is updated.

## 1.0.2: 2014-04-29

The most important topic on this release is that the core component aka Droonga Engine becomes fluentd-free.
As the result, the project (and gem package) `fluent-plugin-droonga` is renamed to `droonga-engine`.
Of course the compatibility of the protocol is still there.

 * Becomes fluentd-free.
 * Supports new `--daemon` and `--pid-file` options for the daemon mode.
 * More Groonga-compatible features are available:
   * `table_list` command
   * `column_list` command
   * `column_remove` command
   * `column_rename` command
   * `delete` command
   * options for `select` command
     * `filter`
     * `sortby`
     * `drilldown`
     * `drilldown_output_columns`
     * `drilldown_sortby`
     * `drilldown_offset`
     * `drilldown_limit`
 * A useful command line tool `droonga-catalog-generate` is included.
   It helps you to write your custom `catalog.json`.
 * Parameters for the `search` command is validated more strictly.
 * The default port number is changed from 24224 (fluentd's one) to 10031.

## 1.0.1: 2014-03-29

### Improvements

  * More documents around plugin APIs are now available.
    See the plugin [development tutorial](http://droonga.org/tutorial/plugin-development/) and the [plugin API reference](http://droonga.org/reference/plugin/).
  * Some documented features of the `catalog.json` are actually implemented.
    For example:
    * A new `vectorOptions` option for a schema.
    * New options `fact`, `dimension`, `nWorkers` and so on.
    For more details, see [the reference of the `catalog.json`](http://droonga.org/reference/catalog/).
  * Connections to other Droonga Engine nodes are automatically re-established correctly.
  * Some improvements about the [`search` command](http://droonga.org/reference/commands/search/)
    * The column name `_nsubrecs` is available as a source with `groupBy` and `sortBy`.
    * The element `elapsedTime` is now available for a value of `elements`.
    * A new parameter `adjusters` is introduced. (Not documented yet, so see also [Groonga's document](http://groonga.org/docs/reference/commands/select.html#select-adjuster))
    * `groupBy` becomes faster.
  * And some small bugfixes.

## 1.0.0: 2014-02-28

### Improvements

  * Updated catalog.json specification to
    [version2](http://droonga.org/reference/catalog/version2/).
    [version1](http://droonga.org/reference/catalog/version1/) is
    still usable. But It is deprecated.
  * Supported log API in plugin.
  * Supported auto catalog.json reload.
  * Changed adapter API:

    Old:

        message.input_pattern  = []
        message.output_pattern = []

    New:

        input_message.pattern  = []
        output_message.pattern = []

  * Supported developing a plugin for handling phase.
    See [tutorial](http://droonga.org/tutorial/plugin-development/handler/)
    for details.

## 0.9.9: 2014-02-09

### Improvements

  * Supported gathering errors.
  * Added more error handled cases.
  * experimental: Added a MeCab filter that filters results from
    N-gram tokenizer based search by MeCab based tokenized search. It
    is disabled by default. You need to define
    `DROONGA_ENABLE_SEARCH_MECAB_FILTER=yes` environment variable when
    you run fluentd.
  * Supported developing a plugin. You can custom adaption phase for now.
    See [tutorial](http://droonga.org/tutorial/plugin-development/) for details.

## 0.9.0: 2014-01-29

### Improvements

  * `search`: Supported `"attributes"` for `elements` of `output`.
  * `table_remove`: Implemented Groonga compatible `table_remove`
    command.
  * `column_create`: Implemented error handling.
  * `catalog`: Supported auto reloading.
  * Supported reducing responses from two or more nodes for Groonga
    compatible commands.
  * Supported three or more partitions.

## 0.8.0: 2013-12-29

### Improvements

  * `search`: Supported `groupBy` with multiple partitions.
  * Changed job queue implementation to UNIX domain socket based
    implementation from Groonga's queue based implementation. It
    reduces shutdown time and fixes job queue break problem on
    crash.
  * Supported error response.
  * `watch`: Fixed a bug that duplicated notification is pushed when
    multiple column values are matched.
  * `watch`: Supported subscriber garbage collection.
  * Improved plugin API. Plugin API documentation will be published
    in the next release.
  * Added micro seconds information to time value. It uses W3C-DTF format
    such as `2013-12-29T00:00:00.000000Z`.
  * Changed the name of adapter plugin that provides Groonga's `select`
    compatible API to `groonga` from `select`. The `groonga` adapter will
    provide more Groonga compatible commands.
  * Added `version` to `catalog.json`.
  * Changed plugin path format to
    `droonga/plugin/#{PLUGIN_TYPE}/#{PLUGIN_NAME}.rb` from
    `droonga/plugin/#{PLUGIN_TYPE}_#{PLUGIN_NAME}.rb`. We use a
    directory per plugin type instead of putting all plugins to
    `droonga/plugin/` directory. Because it is more maintainable.
  * Split adapter plugin into input adapter plugin and output adapter
    plugin. You can in the next release.

## 0.7.0: 2013-11-29

The first release!!!
