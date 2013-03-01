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

require "helper"

require "droonga/worker"

class WorkerTest < Test::Unit::TestCase
  def setup
    setup_database
    setup_worker
  end

  def teardown
    teardown_worker
  end

  private
  def setup_database
    restore(fixture_data("document.grn"))
  end

  def setup_worker
    @worker = Droonga::Worker.new(@database_path.to_s, "DroongaQueue")
  end

  def teardown_worker
    @worker.shutdown
    @worker = nil
  end

  private
  class SearchTest < self
    def test_minimum
      request = {
        "type" => "search",
        "body" => {
          "queries" => {
            "sections" => {
              "source" => "Sections",
            },
          }
        },
      }
      expected = {
        "sections" => {
          "startTime" => start_time,
          "elapsedTime" => elapsed_time,
          "count" => 9,
          "attributes" => [
            {"name" => "content", "type" => "Text",      "vector" => false},
            {"name" => "title",   "type" => "ShortText", "vector" => false},
          ],
          "records" => [
            [
              "Groonga is a fast and accurate full text search engine based on inverted index. One of the characteristics of groonga is that a newly registered document instantly appears in search results. Also, groonga allows updates without read locks. These characteristics result in superior performance on real-time applications.",
              "Groonga overview",
            ],
            [
              "In widely used DBMSs, updates are immediately processed, for example, a newly registered record appears in the result of the next query. In contrast, some full text search engines do not support instant updates, because it is difficult to dynamically update inverted indexes, the underlying data structure.",
              "Full text search and Instant update",
            ],
            [
              "People can collect more than enough data in the Internet era. However, it is difficult to extract informative knowledge from a large database, and such a task requires a many-sided analysis through trial and error. For example, search refinement by date, time and location may reveal hidden patterns. Aggregate queries are useful to perform this kind of tasks.",
              "Column store and aggregate query",
            ],
            [
              "An inverted index is a traditional data structure used for large-scale full text search. A search engine based on inverted index extracts index terms from a document when it is added. Then in retrieval, a query is divided into index terms to find documents containing those index terms. In this way, index terms play an important role in full text search and thus the way of extracting index terms is a key to a better search engine.",
              "Inverted index and tokenizer",
            ],
            [
              "Multi-core processors are mainstream today and the number of cores per processor is increasing. In order to exploit multiple cores, executing multiple queries in parallel or dividing a query into sub-queries for parallel processing is becoming more important.",
              "Sharable storage and read lock-free",
            ],
            [
              "Location services are getting more convenient because of mobile devices with GPS. For example, if you are going to have lunch or dinner at a nearby restaurant, a local search service for restaurants may be very useful, and for such services, fast geo-location search is becoming more important.",
              "Geo-location (latitude and longitude) search",
            ],
            [
              "The basic functions of groonga are provided in a C library and any application can use groonga as a full text search engine or a column-oriented database. Also, libraries for languages other than C/C++, such as Ruby, are provided in related projects. See related projects for details.",
              "Groonga library",
            ],
            [
              "Groonga provides a built-in server command which supports HTTP, the memcached binary protocol and the groonga query transfer protocol (gqtp). Also, a groonga server supports query caching, which significantly reduces response time for repeated read queries. Using this command, groonga is available even on a server that does not allow you to install new libraries.",
              "Groonga server",
            ],
            [
              "Groonga works not only as an independent column-oriented DBMS but also as storage engines of well-known DBMSs. For example, mroonga is a MySQL pluggable storage engine using groonga. By using mroonga, you can use groonga for column-oriented storage and full text search. A combination of a built-in storage engine, MyISAM or InnoDB, and a groonga-based full text search engine is also available. All the combinations have good and bad points and the best one depends on the application. See related projects for details.",
              "Groonga storage engine",
            ],
          ],
        },
      }
      actual = @worker.process_message(request)
      assert_equal(expected, normalize_result_set(actual))
    end

    private
    def start_time
      "2013-01-31T14:34:47+09:00"
    end

    def elapsed_time
      0.01
    end

    def normalize_result_set(result_set)
      normalized_result_set = copy_deeply(result_set)
      normalized_result_set.each do |name, result|
        result["startTime"] = start_time if result["startTime"]
        result["elapsedTime"] = elapsed_time if result["elapsedTime"]
      end
      normalized_result_set
    end

    def copy_deeply(object)
      Marshal.load(Marshal.dump(object))
    end
  end
end
