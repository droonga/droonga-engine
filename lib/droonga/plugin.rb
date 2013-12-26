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

require "droonga/plugin_registerable"

module Droonga
  class Plugin
    def initialize
    end

    def start
    end

    def shutdown
    end

    def processable?(command)
      self.class.processable?(command)
    end

    def process(command, *arguments)
      __send__(self.class.method_name(command), *arguments)
    rescue => exception
      process_error(command, exception, arguments)
    end
    
    private
    def process_error(command, error, arguments)
      Logger.error("error while processing #{command}",
                   error,
                   :arguments => arguments)
    end
  end
end
