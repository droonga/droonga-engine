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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "time"

require "droonga/replier"
require "droonga/forwarder"
require "droonga/timestamp"

module Droonga
  class HandlerMessenger
    attr_reader :database_name, :dispatcher, :engine_state, :cluster

    def initialize(forwarder, message, options={})
      @forwarder = forwarder
      @message   = message
      @options   = options
      @replier   = Replier.new(@forwarder)
      @dispatcher    = options[:dispatcher]
      @engine_state  = options[:engine_state]
      @cluster       = options[:cluster]
      @database_name = options[:database]
    end

    def emit(value)
      descendants = @message.descendants
      raw_message = @message.raw
      if descendants.empty?
        return if raw_message["replyTo"].nil?
        @replier.reply(raw_message.merge("body" => value))
      else
        descendants.each do |name, dests|
          body = {
            "id"    => @message.id,
            "input" => name,
            "value" => value[name],
          }
          dests.each do |dest|
            if @dispatcher
              @dispatcher.dispatch(body, dest)
            else
              message = raw_message.merge("body" => body)
              forward(message, "to" => dest, "type" => "dispatcher")
            end
          end
        end
      end
    end

    def error(status_code, body)
      descendants = @message.descendants
      raw_message = @message.raw
      if descendants.empty?
        return if raw_message["replyTo"].nil?
        response = raw_message.merge("statusCode" => status_code,
                                     "body" => body)
        @replier.reply(response)
      else
        #XXX This is just a temporary solution. We should rewrite this,
        #    to put errors outside of the message body. Otherwise
        #    plugins cannot use the name "errors" for their message body.
        body = {
          "id"    => @message.id,
          "input" => "errors",
          "value" => {
            database_name => {
              "statusCode" => status_code,
              "body" => body,
            },
          },
        }
        all_dests = []
        descendants.each do |name, dests|
          all_dests += dests
        end
        all_dests.each do |dest|
          if @dispatcher
            @dispatcher.dispatch(body, dest)
          else
            message = raw_message.merge("statusCode" => status_code,
                                        "body" => body,)
            forward(message, "to" => dest, "type" => "dispatcher")
          end
        end
      end
    end

    # Forwards a Droonga message to other Droonga Engine.
    #
    # @param [Hash] droonga_message
    #   The Droonga message to be forwarded.
    # @param [Hash] destination
    #   The destination of the Droonga message. See {Forwarder#forward} to
    #   know about how to specify destination.
    #
    # @return [void]
    #
    # @see Forwarder#forward
    def forward(droonga_message, destination)
      droonga_message["date"] ||= new_date
      @forwarder.forward(droonga_message, destination)
    end

    def inspect
      "\#<#{self.class} id=#{object_id}>"
    end

    private
    def log_tag
      "[#{Process.ppid}] handler_messenger"
    end

    def new_date
      Timestamp.stringify(Time.now)
    end
  end
end
