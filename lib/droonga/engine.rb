# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Droonga Project
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

require "droonga/logger"
require "droonga/dispatcher"

module Droonga
  class Engine
    def initialize(options={})
      @options = options
      @dispatcher = Dispatcher.new(@options)
    end

    def start
      @dispatcher.start
    end

    def shutdown
      $log.trace("engine: shutdown: start")
      @dispatcher.shutdown
      $log.trace("engine: shutdown: done")
    end

    def emit(tag, time, record)
      $log.trace("[#{Process.pid}] tag: <#{tag}> caller: <#{caller.first}>")
      @dispatcher.handle_envelope(parse_record(tag, record))
    end

    private
    def parse_record(tag, record)
      prefix, type, *arguments = tag.split(/\./)
      if type.nil? || type.empty? || type == 'message'
        envelope = record
      else
        envelope = {
          "type" => type,
          "arguments" => arguments,
          "body" => record
        }
      end
      envelope["via"] ||= []
      reply_to = envelope["replyTo"]
      if reply_to.is_a? String
        envelope["replyTo"] = {
          "type" => envelope["type"] + ".result",
          "to" => reply_to
        }
      end
      envelope
    end
  end
end
