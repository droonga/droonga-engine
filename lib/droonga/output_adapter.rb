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

require "droonga/pluggable"
require "droonga/output_adapter_plugin"
require "droonga/output_message"

module Droonga
  class OutputAdapter
    include Pluggable

    def initialize(dispatcher, options={})
      @dispatcher = dispatcher
      load_plugins(options[:plugins] || [])
    end

    def adapt(message)
      adapted_message = message

      output_message = OutputMessage.new(adapted_message)
      adapt_errors(output_message)
      adapted_message = output_message.adapted_message

      message["via"].reverse_each do |command|
        @plugins.each do |plugin|
          next unless plugin.processable?(command)
          output_message = OutputMessage.new(adapted_message)
          process(command, output_message)
          adapted_message = output_message.adapted_message
        end
      end
      adapted_message
    end

    private
    #XXX This is just a temporary solution. We should handle errors without "body", for safety.
    def adapt_errors(output_message)
      if output_message.body.include?("errors")
        errors = output_message.body["errors"]
        if errors && !errors.empty?
          output_message.errors = errors

          status_codes = []
          errors.values.each do |error|
            status_codes << error["statusCode"]
          end
          status_codes = status_codes.uniq
          if status_codes.size == 1
            output_message.status_code = status_codes.first
          else
            output_message.status_code = MessageProcessingError::STATUS_CODE
          end

          output_message.body = errors.values.first["body"]
        else
          output_message.body.delete("errors")
        end
      end
    end

    def instantiate_plugin(name)
      OutputAdapterPlugin.repository.instantiate(name, @dispatcher)
    end

    def log_tag
      "output-adapter"
    end
  end
end
