# Copyright (C) 2014-2015 Droonga Project
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "fiber"
require "coolio"

require "droonga/loggable"
require "droonga/handler"
require "droonga/error_messages"

module Droonga
  module Plugins
    module AsyncCommand
      class Request
        def initialize(message)
          @message = message
        end

        def need_start?
          reply_to
        end

        def id
          @message["id"]
        end

        def dataset
          @message.raw["dataset"]
        end

        def reply_to
          (@message.raw["replyTo"] || {})["to"]
        end

        def request
          @message.request
        end

        DEFAULT_MESSAGES_PER_SECOND = 10000

        def messages_per_seconds
          request = (@message.request || {})
          minimum_messages_per_seconds = 10
          [
            minimum_messages_per_seconds,
            (request["messagesPerSecond"] || DEFAULT_MESSAGES_PER_SECOND).to_i,
          ].max
        end
      end

      class Handler < Droonga::Handler
        def handle(message)
          request = Request.new(message)
          if request.need_start?
            start(request)
            {
              "started" => true,
            }
          else
            {
              "started" => false,
            }
          end
        end

        private
        def start(request)
          #XXX override me!
          # handler = MyAsyncHandler.new(loop, messenger, request)
          # handler.start
        end
      end

      class AsyncHandler
        include Loggable

        def initialize(loop, messenger, request)
          @loop = loop
          @messenger = messenger
          @request = request
        end

        def start
          setup_forward_data

          forward("#{prefix}.start")

          runner = Fiber.new do
            handle
          end

          timer = Coolio::TimerWatcher.new(0.1, true)
          timer.on_timer do
            if runner.alive?
              begin
                runner.resume
              rescue
                timer.detach
                logger.trace("start: watcher detached on unexpected exception",
                             :watcher => timer)
                logger.exception(error_message, $!)
                error(error_name, error_message)
              end
            else
              timer.detach
            end
          end

          @loop.attach(timer)
          logger.trace("start: new watcher attached",
                       :watcher => timer)
        end

        private
        def prefix
          "" #XXX override me!!
        end

        def handle
          #XXX override me!!
          forward("#{prefix}.end") #XXX you must forward "end" message by self!
        end

        def setup_forward_data
          @base_forward_message = {
            "inReplyTo" => @request.id,
            "dataset"   => @request.dataset,
          }
          @forward_to = @request.reply_to
          @n_forwarded_messages = 0
          @messages_per_100msec = @request.messages_per_seconds / 10
        end

        def error_name
          "Failure" #XXX override me!!
        end

        def error_message
          "failed to do" #XXX override me!!
        end

        def error(name, message)
          message = {
            "statusCode" => ErrorMessages::InternalServerError::STATUS_CODE,
            "body" => {
              "name"    => name,
              "message" => message,
            },
          }
          error_message = @base_forward_message.merge(message)
          @messenger.forward(error_message,
                             "to"   => @forward_to,
                             "type" => "#{prefix}.error")
        end

        def forward(type, body=nil)
          forward_message = @base_forward_message
          if body
            forward_message = forward_message.merge("body" => body)
          end
          @messenger.forward(forward_message,
                             "to"   => @forward_to,
                             "type" => type)

          @n_forwarded_messages += 1
          @n_forwarded_messages %= @messages_per_100msec
          Fiber.yield if @n_forwarded_messages.zero?
        end

        def log_tag
          "[#{Process.ppid}] async-handler"
        end
      end
    end
  end
end
