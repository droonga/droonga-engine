# -*- coding: utf-8 -*-
#
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

require "droonga/handler"

module Droonga
  class MergeHandler < Droonga::Handler
    Droonga::HandlerPlugin.register("forward", self)

    CONFIG_FILE_PATH = 'config.json'

    def handlable?(command)
      true
    end

    def handle(command, request, *arguments)
      destination = get_destination
      post(request,
           "to" => destination, "type" => command, "arguments" => arguments)
    rescue => exception
      if $log
        $log.error "error while handling #{command}",
          request: request,
          arguments: arguments,
          exception: exception
        $log.error_backtrace
      end
    end

    def get_destination
      loop do
        refresh_config
        if @config && @config["forward"]
          path = @context.database.path
          destination = @config["forward"][path]
          return destination unless destination.nil? || destination.empty?
        end
        sleep 5
      end
    end

    def refresh_config
      unless File.exists?(CONFIG_FILE_PATH)
        @config = nil
        return
      end
      mtime = File.mtime(CONFIG_FILE_PATH)
      return if @config_mtime == mtime
      open(CONFIG_FILE_PATH) do |file|
        @config = JSON.parse(file.read)
      end
      @config_mtime = mtime
    end
  end
end
