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
    module Level
      TRACE = 0
      DEBUG = 1
      INFO  = 2
      WARN  = 3
      ERROR = 4
      FATAL = 5

      LABELS = [
        "trace",
        "debug",
        "info",
        "warn",
        "error",
        "fatal",
      ]

      class << self
        def lable(level)
          LABELS[level]
        end
      end
    end

    def initialize(options={})
      @output = options[:output] || $stdout
      @tag = options[:tag]
      @level = Level::INFO
    end

    def level
      Level.label(@level)
    end

    def level=(level)
      @level = Level::LABELS.index(level.to_s)
    end

    def trace(message, data={})
      log(Level::TRACE, message, data)
    end

    def debug(message, data={})
      log(Level::DEBUG, message, data)
    end

    def info(message, data={})
      log(Level::INFO, message, data)
    end

    def warn(message, data={})
      log(Level::WARN, message, data)
    end

    def error(message, data={})
      log(Level::ERROR, message, data)
    end

    def exception(message, exception, data={})
      log(Level::Error,
          "#{message}: #{exception.message}(#{exception.class})",
          data)
      log_backtrace(Level::Error, exception.backtrace)
    end

    private
    def log(level, message, data)
      return unless target_level?(level)
      @output.print(build_log_line(message, data))
    end

    def log_backtrace(level, backtrace)
      return unless target_level?(level)
      backtrace.each do |message|
        @output.write(build_log_line(message))
      end
    end

    def target_level?(level)
      @level <= level
    end

    def build_log_line(level, message, data={})
      line = "#{Time.now.iso8601}[#{Level.label(level)}]: "
      line << "#{@tag}: " if @tag
      line << message
      data.each do |key, value|
        line << " #{key}=#{value.inspect}"
      end
      line << "\n"
      line
    end
  end
end
