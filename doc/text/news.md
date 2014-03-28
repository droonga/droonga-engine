# News

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
