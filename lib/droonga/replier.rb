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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "droonga/loggable"
require "droonga/status_code"

module Droonga
  class Replier
    include Loggable

    def initialize(forwarder)
      @forwarder = forwarder
    end

    def reply(message)
      logger.trace("reply: start")
      destination = message["replyTo"]
      reply_message = {
        "inReplyTo"  => message["id"],
        "statusCode" => message["statusCode"] || StatusCode::OK,
        "type"       => destination["type"],
        "body"       => message["body"],
      }
      if message.include?("errors")
        errors = message["errors"]
        reply_message["errors"] = errors unless errors.empty?
      end
      @forwarder.forward(reply_message, destination)
      logger.trace("reply: done")
    end

    private
    def log_tag
      "[#{Process.ppid}] replier"
    end
  end
end
