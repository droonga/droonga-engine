# -*- coding: utf-8 -*-
#
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

module Droonga
  class << self
    def logger
      @logger ||= Logger.new
    end

    def logger=(logger)
      @logger = logger
    end
  end

  class Logger
    def initialize(options={})
      @output = Fluent::PluginLogger.new($log)
      depth_offset = options[:depth_offset] || 0
      @output.instance_variable_set(:@depth_offset, 4 + depth_offset)
      @tag = options[:tag]
    end

    def level
      @output.level
    end

    def trace(message, data={})
      log(:trace, message, data)
    end

    def debug(message, data={})
      log(:debug, message, data)
    end

    def info(message, data={})
      log(:info, message, data)
    end

    def error(message, data={})
      log(:error, message, data)
    end

    def exception(message, exception, data={})
      log(:error,
          "#{message}: #{exception.message}(#{exception.class})",
          data)
      @output.error_backtrace(exception.backtrace)
    end

    private
    def log(level, message, data)
      message = "#{@tag}: #{message}" if @tag
      arguments = [message]
      arguments << data unless data.empty?
      @output.send(level, *arguments)
    end
  end
end
