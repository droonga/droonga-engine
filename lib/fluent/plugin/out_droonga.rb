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

require "droonga/engine"
require "droonga/plugin_loader"

module Fluent
  class DroongaOutput < Output
    Plugin.register_output("droonga", self)

    config_param :name, :string, :default => ""

    def start
      super
      Droonga::PluginLoader.load_all
      @engine = Droonga::Engine.new(:name => @name)
      @engine.start
    end

    def shutdown
      @engine.shutdown
      super
    end

    def emit(tag, es, chain)
      es.each do |time, record|
        process_event(tag, record)
      end
      chain.next
    end

    private
    def process_event(tag, record)
      $log.trace("out_droonga: tag: <#{tag}> caller: <#{caller.first}>")
      @engine.process(parse_record(tag, record))
    end

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
