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
    setup_handlers
    setup_worker
    setup_output_receiver
  end

  def teardown
    teardown_worker
    teardown_output_receiver
  end

  private
  def setup_database
    restore(fixture_data("document.grn"))
  end

  def setup_handlers
    ["search"].each do |handler_name|
      plugin = Droonga::Plugin.new("handler", handler_name)
      plugin.load
    end
  end

  def setup_worker
    @worker = Droonga::Worker.new(@database_path.to_s, "DroongaQueue")
    @worker.add_handler("search")
  end

  def teardown_worker
    @worker.shutdown
    @worker = nil
  end

  def setup_output_receiver
    @output_receiver_host = "127.0.0.1"
    @output_receiver_port = 2929
    @output_receiver = TCPServer.new(@output_receiver_host,
                                     @output_receiver_port)
  end

  def teardown_output_receiver
    @output_receiver.close
    @output_receiver = nil
  end

  class SearchTest < self
    def test_minimum
      request = {
        "type" => "search",
        "id" => request_id,
        "replyTo" => reply_to,
        "body" => {
          "queries" => {
            "sections" => {
              "source" => "Sections",
              "output" => {
                "count" => true,
              },
            },
          },
        },
      }
      expected = {
        "inReplyTo" => request_id,
        "type" => "search.result",
        "statusCode" => 200,
        "body" => {
          "sections" => {
            "count" => 9,
          },
        },
      }
      @worker.process_message(request)
      actual = receive_response
      assert_equal(expected, normalize_result_set(actual))
    end

    private
    def start_time
      "2013-01-31T14:34:47+09:00"
    end

    def elapsed_time
      0.01
    end

    def reply_to
      "#{@output_receiver_host}:#{@output_receiver_port}/droonga.message"
    end

    def request_id
      "request-id"
    end

    def normalize_result_set(result_set)
      normalized_result_set = copy_deeply(result_set)
      normalized_result_set["body"].each do |name, result|
        result["startTime"] = start_time if result["startTime"]
        result["elapsedTime"] = elapsed_time if result["elapsedTime"]
      end
      normalized_result_set
    end

    ENOUGH_RESPONSE_DATA_SIZE = 4096 * 4
    def receive_response
      readables, = IO.select([@output_receiver], [], [], 0)
      assert_not_empty(readables, "not replied")

      response_socket = @output_receiver.accept
      response_data = response_socket.read_nonblock(ENOUGH_RESPONSE_DATA_SIZE)
      tag, time, response = MessagePack.unpack(response_data)
      response
    end

    def copy_deeply(object)
      Marshal.load(Marshal.dump(object))
    end
  end
end
