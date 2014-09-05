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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

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
        def label(level)
          LABELS[level]
        end

        def value(label)
          LABELS.index(label.to_s)
        end

        def default
          ENV["DROONGA_LOG_LEVEL"] || label(WARN)
        end
      end
    end

    class << self
      @@default_output = nil
      def default_output
        @@default_output || $stdout
      end

      def default_output=(output)
        @@default_output = output
      end
    end

    def initialize(options={})
      @output = options[:output] || self.class.default_output
      @tag = options[:tag]
      self.level = options[:level] || Level.default
    end

    def level
      Level.label(@level)
    end

    def level=(level)
      if level.is_a?(Numeric)
        @level = level
      else
        @level = Level.value(level)
      end
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
      log(Level::ERROR,
          "#{message}: #{exception.class}: #{exception.message}",
          data)
      log_backtrace(Level::ERROR, exception.backtrace)
    end

    private
    def log(level, message, data)
      return unless target_level?(level)
      @output.print(build_log_line(level, message, data))
      @output.flush
    end

    def log_backtrace(level, backtrace)
      return unless target_level?(level)
      backtrace.each do |message|
        @output.write(build_log_line(level, message))
      end
    end

    def target_level?(level)
      @level <= level
    end

    def build_log_line(level, message, data={})
      line = "#{Time.now.iso8601}[#{Process.pid}][#{Level.label(level)}]: "
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
