# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 droonga project
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

require "json"

module DroongaBenchmark
  WATCH_DATASET = "Watch"

  class WatchDatabase
    attr_reader :context

    def initialize
      @database_dir = "/tmp/watch-benchmark"
      @database_path = "#{@database_dir}/db"
      @ddl_path = File.expand_path(File.join(__FILE__, "..", "..", "ddl", "watchdb.grn"))
      FileUtils.rm_rf(@database_dir)
      FileUtils.mkdir_p(@database_dir)

      Groonga::Database.create(:path => @database_path)

      @context = Groonga::Context.new
      @context.open_database(@database_path)
      File.open(@ddl_path) do |ddl|
        @context.restore(ddl)
      end
    end

=begin
    def subscribe_to(keywords)
      @context.send("load --table Query")
      @context.send("[")
      keywords.each do |keyword|
        @context.send("{'_key':'#{keyword}'," +
                        "'keywords':['#{keyword}']},")
      end
      @context.send("]")

      @context.send("load --table Subscriber")
      @context.send("[")
      keywords.each do |keyword|
        @context.send("{'_key':'subscriber for #{keyword}'," +
                        "'subscriptions':['#{keyword}']," +
                        "'route':'0.0.0.0:0/benchamrk'},")
      end
      @context.send("]")
    end
=end

    def subscribe_to(keywords)
      queries = []
      subscribers = []
      keywords.each do |keyword|
        queries << {:_key => keyword,
                    :keywords => [keyword]}
        subscribers << {:_key => "subscriber for #{keyword}",
                        :subscriptions => [keyword],
                        :route => "0.0.0.0:0/benchamrk"}
      end

      command_load_queries = [
        "load --table Query",
        JSON.generate(queries)
      ]
      command_load_subscribers = [
        "load --table Subscriber",
        JSON.generate(subscribers)
      ]

      @context.restore(command_load_queries.join("\n"))
      @context.restore(command_load_subscribers.join("\n"))
    end

    def subscribe(keyword)
      queries = @context["Query"]
      query = queries.add(keyword, :keywords => [keyword])

      subscribers = @context["Subscriber"]
      subscribers.add("subscriber for #{keyword}",
                      :subscriptions => [query],
                      :route => "0.0.0.0:0/benchamrk")
    end
  end

  class KeywordsGenerator
    class << self
      def generate(n_keywords)
        new.generate(n_keywords)
      end
    end

    def initialize
      @generator = to_enum(:each)
    end

    def generate(n_keywords)
      keywords = []
      n_keywords.times do
        keywords << @generator.next
      end
      keywords
    end

    def next
      @generator.next
    end

    FIRST_INITIAL_LETTER = "㐀"
    SUFFIX = "あいうえおかきくけこ"
    def each
      initial_letter = FIRST_INITIAL_LETTER
      while true do
        yield "#{initial_letter}#{SUFFIX}"
        initial_letter.succ!
      end
    end
  end

  class TargetsGenerator
    class << self
      def generate(n_keywords, params)
        new(params).generate(n_keywords)
      end
    end

    PADDING = "パディング"
    SIZE    = 1000

    def initialize(params)
      @keywords  = params[:keywords]
      @incidence = params[:incidence]
    end

    def generate(n_targets)
      targets = []

      n_matched_targets = (n_targets.to_f * @incidence).to_i
      n_unmatched_targets = (n_targets - n_matched_targets)

      n_matched_targets.times do
        targets << generate_target(@keywords.sample(1).first)
      end

      n_unmatched_targets.times do
        targets << generate_target
      end

      targets
    end

    def generate_target(keyword="")
     (PADDING * (SIZE / PADDING.size)) + keyword
    end
  end

  class MessageCreator
    class << self
      def envelope_to_subscribe(keyword, route=nil)
        message = {
          "id" => Time.now.to_f.to_s,
          "dataset" => WATCH_DATASET,
          "date" => Time.now,
          "statusCode" => 200,
          "type" => "watch.subscribe",
          "body" => {
            "condition" => keyword,
            "subscriber" => "subscriber for #{keyword}",
          },
        }
        message
      end

      def envelope_to_feed(keyword)
        {
          "id" => Time.now.to_f.to_s,
          "dataset" => WATCH_DATASET,
          "date" => Time.now,
          "statusCode" => 200,
          "type" => "watch.feed",
          "body" => {
            "targets" => {
              "keyword"  => keyword,
            },
          },
        }
      end
    end
  end

  class MessageReceiver
    def initialize(options={})
      default_options = {
        :host => "0.0.0.0",
        :port => 0,
      }
      options = default_options.merge(options)
      @socket = TCPServer.new(options[:host], options[:port])
    end

    def close
      @socket.close
    end

    def host
      @socket.addr[3]
    end

    def port
      @socket.addr[1]
    end

    def receive(options={})
      if IO.select([@socket], nil, nil, options[:timeout])
        client = @socket.accept
        message = nil
        unpacker = MessagePack::Unpacker.new(client)
        unpacker.each do |object|
          message = object
          break
        end
        client.close
        message
      else
        nil
      end
    end
  end
end
