# News

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
