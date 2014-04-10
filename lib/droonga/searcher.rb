# Copyright (C) 2013-2014 Droonga Project
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1 as published by the Free Software Foundation.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

require "English"
require "tsort"
require "groonga"

require "droonga/loggable"
require "droonga/error_messages"

module Droonga
  class Searcher
    include Loggable

    class NoQuery < ErrorMessages::BadRequest
      def initialize
        super("You must specify one or more query.")
      end
    end

    class MissingSourceParameter < ErrorMessages::BadRequest
      def initialize(query, queries)
        super("[#{query}] No source is specified. " +
                "Query must have a valid source.",
              queries)
      end
    end

    class UnknownSource < ErrorMessages::NotFound
      def initialize(source, queries)
        super("Source not found: <#{source}> " +
                "It must be a name of an existing table or another query.",
              queries)
      end
    end

    class CyclicSource < ErrorMessages::BadRequest
      def initialize(queries)
        super("There is cyclic reference of sources.",
              queries)
      end
    end

    class InvalidAttribute < ErrorMessages::BadRequest
      attr_reader :attribute
      def initialize(attribute)
        detail = {
          "attribute" => attribute,
        }
        super("Invalid attribute: <#{attribute}>", detail)
      end
    end

    class SyntaxError < ErrorMessages::BadRequest
      attr_reader :syntax
      attr_reader :input
      def initialize(syntax, input)
        detail = {
          "syntax" => syntax,
          "input" => input,
        }
        super("Syntax error: syntax:<#{syntax}> input:<#{input}>", detail)
      end
    end

    def initialize(context)
      @context = context
    end

    def search(queries)
      outputs = nil
      logger.trace("search: start", :queries => queries)
      # TODO: THIS IS JUST A WORKAROUND! We should remove it ASAP!
      disable_gc do
        @context.push_memory_pool do
          outputs = process_queries(queries)
        end
      end
      logger.trace("search: done")
      return outputs
    end

    private
    def disable_gc
      GC.disable
      begin
        yield
      ensure
        GC.enable
      end
    end

    def process_queries(queries)
      logger.trace("process_queries: start")
      if queries.nil? or queries.empty?
        raise NoQuery.new
      end
      logger.trace("process_queries: sort: start")
      sorted_queries = QuerySorter.sort(queries)
      logger.trace("process_queries: sort: done")
      outputs = {}
      results = {}
      sorted_queries.each do |name|
        if queries[name]
          logger.trace("process_queries: search: start",
                       :name => name)
          search_request = SearchRequest.new(@context, queries[name], results)
          search_result = QuerySearcher.search(search_request)
          results[name] = search_result.records
          logger.trace("process_queries: search: done",
                       :name => name)
          if search_request.need_output?
            logger.trace("process_queries: format: start",
                         :name => name)
            outputs[name] = ResultFormatter.format(search_request, search_result)
            logger.trace("process_queries: format: done",
                         :name => name)
          end
        elsif @context[name]
          results[name] = @context[name]
        else
          raise UnknownSource.new(name, queries)
        end
      end
      logger.trace("process_queries: done")
      return outputs
    end

    def log_tag
      "[#{Process.ppid}][#{Process.pid}] searcher"
    end

    class QuerySorter
      include TSort

      class << self
        def sort(queries)
          query_sorter = new
          queries.each do |name, query|
            source = query["source"]
            raise MissingSourceParameter.new(name, queries) unless source
            raise CyclicSource.new(queries) if name == source
            query_sorter.add(name, [source])
          end
          begin
            sorted_queries = query_sorter.tsort
          rescue TSort::Cyclic
            raise CyclicSource.new(queries)
          end
          sorted_queries
        end

        def validate_dependencies(queries)
          sort(queries)
        end
      end

      def initialize()
        @queries = {}
      end

      def add(name, sources=[])
        @queries[name] = sources
      end

      def tsort_each_node(&block)
        @queries.each_key(&block)
      end

      def tsort_each_child(node, &block)
        if @queries[node]
          @queries[node].each(&block)
        end
      end
    end

    class SearchRequest
      attr_reader :context, :query, :resolved_results

      def initialize(context, query, resolved_results)
        @context = context
        @query = query
        @resolved_results = resolved_results
      end

      def need_output?
        @query.has_key?("output")
      end

      def output
        @query["output"]
      end

      def complex_output?
        output["format"] == "complex"
      end

      def source
        source_name = @query["source"]
        @resolved_results[source_name]
      end
    end

    class SearchResult
      attr_accessor :start_time, :end_time, :condition, :records, :count

      def initialize
        @start_time = nil
        @end_time = nil
        @condition = nil
        @records = nil
        @count = nil
      end
    end

    class QuerySearcher
      include Loggable

      OPERATOR_CONVERSION_TABLE = {
        "||" => Groonga::Operator::OR,
        "&&" => Groonga::Operator::AND,
        "-"  => Groonga::Operator::AND_NOT,
      }

      class << self
        def search(search_request)
          new(search_request).search
        end
      end

      def initialize(search_request)
        @result = SearchResult.new
        @request = search_request
      end

      def search
        search_query!
        @result
      end

      private
      def parse_condition(source, expression, condition)
        case condition
        when String
          expression.parse(condition, :syntax => :script)
        when Hash
          parse_condition_hash(source, expression, condition)
        when Array
          parse_condition_array(source, expression, condition)
        else
          raise "unacceptable object #{condition.inspect} assigned"
        end
      end

      def parse_condition_hash(source, expression, condition)
        options = {}
        if condition["matchTo"]
          matchTo = Groonga::Expression.new(context: @request.context)
          matchTo.define_variable(:domain => source)
          match_columns = condition["matchTo"]
          match_columns = match_columns.join(",") if match_columns.is_a?(Array)
          matchTo.parse(match_columns, :syntax => :script)
          options[:default_column] = matchTo
        end
        query = condition["query"]
        if query
          options[:syntax] = :query
          if condition["defaultOperator"]
            default_operator_string = condition["defaultOperator"]
            default_operator = OPERATOR_CONVERSION_TABLE[default_operator_string]
            unless default_operator
              raise "undefined operator assigned #{default_operator_string}"
            end
            options[:default_operator] = default_operator
          end
          if condition["allowPragma"]
            options[:allow_pragma] = true
          end
          if condition["allowColumn"]
            options[:allow_column] = true
          end
          syntax_errors = [
            Groonga::SyntaxError,
            Groonga::InvalidArgument,
            Encoding::CompatibilityError,
          ]
          begin
            expression.parse(query, options)
          rescue *syntax_errors
            raise SyntaxError.new("query", query)
          end
        elsif condition["script"]
          # "script" is ignored when "query" is also assigned.
          options[:syntax] = :script
          if condition["allowUpdate"]
            options[:allow_update] = true
          end
          expression.parse(condition["script"], options)
        else
          raise "neither 'query' nor 'script' assigned in #{condition.inspect}"
        end
      end

      def parse_condition_array(source, expression, condition)
        operator = OPERATOR_CONVERSION_TABLE[condition[0]]
        unless operator
          raise "undefined operator assigned #{condition[0]}"
        end
        if condition[1]
          parse_condition(source, expression, condition[1])
        end
        condition[2..-1].each do |element|
          parse_condition(source, expression, element)
          expression.append_operation(operator, 2)
        end
      end

      def parse_order_keys(keys)
        keys.collect do |key|
          if key =~ /^-/
            [$POSTMATCH, :descending]
          else
            [key, :ascending]
          end
        end
      end

      def search_query!
        logger.trace("search_query: start")

        @result.start_time = Time.now

        @records = @request.source

        condition = @request.query["condition"]
        if condition
          apply_condition!(condition)

          adjusters = @request.query["adjusters"]
          apply_adjusters!(adjusters) if adjusters
        end

        group_by = @request.query["groupBy"]
        apply_group_by!(group_by) if group_by

        @result.count = @records.size

        sort_by = @request.query["sortBy"]
        apply_sort_by!(sort_by) if sort_by

        logger.trace("search_query: done")
        @result.records = @records
        @result.end_time = Time.now
      end

      def apply_condition!(condition)
        expression = Groonga::Expression.new(context: @request.context)
        expression.define_variable(:domain => @records)
        parse_condition(@records, expression, condition)
        logger.trace("search_query: select: start",
                     :condition => condition)
        @records = @records.select(expression)
        logger.trace("search_query: select: done")
        @result.condition = expression
      end

      def apply_adjusters!(adjusters)
        logger.trace("search_query: adjusters: start")
        adjusters.each do |adjuster|
          column_name = adjuster["column"]
          value = adjuster["value"]
          factor = adjuster["factor"] || 1
          logger.trace("search_query: adjusters: adjuster: start",
                       :column_name => column_name,
                       :value => value,
                       :factor => factor)
          column = @request.source.column(column_name)
          index, = column.indexes(:match)
          # TODO: add index.nil? check
          if index.nil?
            # Temporary. It is just for debug on Travis CI.
            logger.error("search_query: adjusters: adjuster: not found index",
                         :column_name => column_name,
                         :value => value,
                         :factor => factor,
                         :column => column,
                         :dump => Groonga::Schema.dump(:context => @request.context,
                                                       :syntax => :command),
                         :indexes => column.indexes(:match))
          end
          # TODO: add value.nil? check
          index.search(value,
                       :result => @records,
                       :operator => :adjust,
                       :weight => factor)
          logger.trace("search_query: adjusters: adjuster: done")
        end
        logger.trace("search_query: adjusters: done")
      end

      def apply_group_by!(group_by)
        logger.trace("search_query: group: start",
                     :by => group_by)
        case group_by
        when String
          @records = @records.group(group_by)
        when Hash
          key = group_by["key"]
          max_n_sub_records = group_by["maxNSubRecords"]
          @records = @records.group(key, :max_n_sub_records => max_n_sub_records)
        else
          raise '"groupBy" parameter must be a Hash or a String'
        end
        logger.trace("search_query: group: done",
                     :by => group_by)
      end

      def apply_sort_by!(sort_by)
        logger.trace("search_query: sort: start",
                     :by => sort_by)
        case sort_by
        when Array
          keys = parse_order_keys(sort_by)
          offset = 0
          limit = -1
        when Hash
          keys = parse_order_keys(sort_by["keys"])
          offset = sort_by["offset"]
          limit = sort_by["limit"]
        else
          raise '"sortBy" parameter must be a Hash or an Array'
        end
        @records = @records.sort(keys, :offset => offset, :limit => limit)
        logger.trace("search_query: sort: done",
                     :by => sort_by)
      end

      def log_tag
        "[#{Process.ppid}][#{Process.pid}] query_searcher"
      end
    end

    module AttributeFormattable
      def format_attribute(attribute, table)
        label = attribute[:label]
        source = attribute[:source]
        if source == "_subrecs"
          sub_record_table = table.range
          sub_attributes = format(attribute[:attributes], sub_record_table)

          format_attribute_subrecs(label, sub_attributes)
        else
          expression = attribute[:expression]
          if expression
            format_attribute_expression(label, expression)
          else
            column = table.column(source)
            format_attribute_column(label, column)
          end
        end
      end
    end

    class SimpleAttributesFormatter
      include AttributeFormattable

      def format_attribute_subrecs(label, sub_attributes)
        {
          "name" => label,
          "attributes" => sub_attributes,
        }
      end

      def format_attribute_column(label, column)
        vector = column.respond_to?(:vector?) ? column.vector? : false
        {"name" => label, "type" => column.range.name, "vector" => vector}
      end

      def format_attribute_expression(label, expression)
        {"name" => label} # TODO include detailed information of expression
      end

      def format(attributes, table)
        attributes.collect do |attribute|
          format_attribute(attribute, table)
        end
      end
    end

    class ComplexAttributesFormatter
      include AttributeFormattable

      def format_attribute_subrecs(label, sub_attributes)
        {
          "attributes" => sub_attributes
        }
      end

      def format_attribute_column(label, column)
        vector = column.respond_to?(:vector?) ? column.vector? : false
        {"type" => column.range.name, "vector" => vector}
      end

      def format_attribute_expression(label, expression)
        {} # TODO include detailed information of expression
      end

      def format(attributes, table)
        formatted_attributes = {}
        attributes.each do |attribute|
          formatted_attribute = format_attribute(attribute, table)
          attribute_name = attribute[:label]
          formatted_attributes[attribute_name] = formatted_attribute
        end
        formatted_attributes
      end
    end

    module RecordsFormattable
      def record_value(record, attribute)
        if attribute[:source] == "_subrecs"
          if record.table.is_a?(Groonga::Array)
            target_record = record.value
          else
            target_record = record
          end
          target_record.sub_records.collect do |sub_record|
            sub_attributes = attribute[:attributes]
            format_record(sub_attributes, sub_record)
          end
        else
          expression = attribute[:expression]
          if expression
            variable = attribute[:variable]
            variable.value = record
            expression.execute
          else
            value = record[attribute[:source]]
            if value.is_a?(Groonga::Record)
              value.record_id
            else
              value
            end
          end
        end
      end

      def format(output_target_attributes, records, output_limit, output_offset)
        cursor_options = {
          :offset => output_offset,
          :limit => output_limit
        }
        formatted_records = nil
        records.open_cursor(cursor_options) do |cursor|
          formatted_records = cursor.collect do |record|
            format_record(output_target_attributes, record)
          end
        end
        formatted_records
      end
    end

    class SimpleRecordsFormatter
      include RecordsFormattable

      def format_record(attributes, record)
        attributes.collect do |attribute|
          record_value(record, attribute)
        end
      end
    end

    class ComplexRecordsFormatter
      include RecordsFormattable

      def format_record(attributes, record)
        values = {}
        attributes.each do |attribute|
          values[attribute[:label]] = record_value(record, attribute)
        end
        values
      end
    end

    class ResultFormatter
      class << self
        def format(search_request, search_result)
          new(search_request, search_result).format
        end
      end

      def initialize(search_request, search_result)
        @request = search_request
        @result = search_result
      end

      def format
        formatted_result = {}

        output_elements.each do |name|
          value = format_element(name)
          next if value.nil?
          formatted_result[name] = value
        end

        formatted_result
      end

      private
      def format_element(name)
        case name
        when "count"
          format_count
        when "attributes"
          format_attributes
        when "records"
          format_records
        when "startTime"
          format_start_time
        when "elapsedTime"
          format_elapsed_time
        else
          nil
        end
      end

      def output_elements
        @request.output["elements"] || []
      end

      def output_offset
        @request.output["offset"] || 0
      end

      def output_limit
        @request.output["limit"] || 10
      end

      def format_count
        @result.count
      end

      def format_attributes
        if @request.complex_output?
          formatter = ComplexAttributesFormatter.new
        else
          formatter = SimpleAttributesFormatter.new
        end
        formatter.format(output_target_attributes, @result.records)
      end

      def output_target_attributes
        attributes = @request.output["attributes"]
        normalize_target_attributes(attributes)
      end

      def format_records
        if @request.complex_output?
          formatter = ComplexRecordsFormatter.new
        else
          formatter = SimpleRecordsFormatter.new
        end
        formatter.format(output_target_attributes, @result.records, output_limit, output_offset)
      end

      def normalize_target_attributes(attributes, domain = @result.records)
        attributes.collect do |attribute|
          if attribute.is_a?(String)
            attribute = {
              "source" => attribute,
            }
          end
          source = attribute["source"]
          if accessor_name?(source)
            expression = nil
            variable = nil
          else
            expression = Groonga::Expression.new(context: @request.context)
            variable = expression.define_variable(domain: domain)
            begin
              expression.parse(source, syntax: :script)
            rescue Groonga::SyntaxError
              raise InvalidAttribute.new(source)
            end
            condition = expression.define_variable(name: "$condition",
                                                   reference: true)
            condition.value = @result.condition
            source = nil
          end
          normalized_attributes = {
            label: attribute["label"] || attribute["source"],
            source: source,
            expression: expression,
            variable: variable,
          }
          if attribute["attributes"]
            normalized_attributes[:attributes] =
              normalize_target_attributes(attribute["attributes"], domain.range)
          end
          normalized_attributes
        end
      end

      def accessor_name?(source)
        /\A[a-zA-Z\#@$_][a-zA-Z\d\#@$_\-.]*\z/ === source
      end

      def format_start_time
        @result.start_time
      end

      def format_elapsed_time
        @result.end_time.to_f - @result.start_time.to_f
      end
    end
  end
end

if ENV["DROONGA_ENABLE_SEARCH_MECAB_FILTER"] == "yes"
  require "droonga/searcher/mecab_filter"
end
