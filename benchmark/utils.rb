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

module DroongaBenchmark
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

    def subscribe_to(terms)
      @context.send("load --table Query")
      @context.send("[")
      terms.each do |term|
        @context.send("{'_key':#{term}," +
                        "'keywords':['#{term}']},")
      end
      @context.send("]")

      @context.send("load --table Subscriber")
      @context.send("[")
      terms.each do |term|
        @context.send("{'_key':'subscriber for #{term}'," +
                        "'subscriptions':['#{term}']," +
                        "'route':'0.0.0.0:0/benchamrk'},")
      end
      @context.send("]")
    end

=begin
# this is slower than above...
    def subscribe_to_with_single_loop(terms)
      queries = []
      subscribers = []
      terms.each do |term|
        queries << {:_key => term,
                    :keywords => [term]}
        subscribers << {:_key => "subscriber for #{term}",
                        :subscriptions => [term],
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
=end

    def subscribe(term)
      queries = @context["Query"]
      query = queries.add(term, :keywords => [term])

      subscribers = @context["Subscriber"]
      subscribers.add("subscriber for #{term}",
                      :subscriptions => [query],
                      :route => "0.0.0.0:0/benchamrk")
    end
  end

  class TermsGenerator
    class << self
      def generate(n_terms)
        new.generate(n_terms)
      end
    end

    def initialize
      @generator = to_enum(:each)
    end

    def generate(n_terms)
      terms = []
      n_terms.times do
        terms << @generator.next
      end
      terms
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
      def generate(n_terms, params)
        new(params).generate(n_terms)
      end
    end

    PADDING = "パディング"
    SIZE    = 1000

    def initialize(params)
      @terms     = params[:terms]
      @incidence = params[:incidence]
    end

    def generate(n_targets)
      targets = []

      n_matched_targets = (n_targets.to_f * @incidence).to_i
      n_unmatched_targets = (n_targets - n_matched_targets)

      n_matched_targets.times do
        targets << generate_target(@terms.sample(1).first)
      end

      n_unmatched_targets.times do
        targets << generate_target
      end

      targets
    end

    def generate_target(term="")
     (PADDING * (SIZE / PADDING.size)) + term
    end
  end
end
