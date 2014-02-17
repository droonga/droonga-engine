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

require "droonga/status_code"

module Droonga
  class Replier
    def initialize(forwarder)
      @forwarder = forwarder
    end

    def reply(message)
      $log.trace("#{log_tag}: reply: start")
      destination = message["replyTo"]
      reply_message = {
        "inReplyTo"  => message["id"],
        "statusCode" => message["statusCode"] || STATUS_OK,
        "type"       => destination["type"],
        "body"       => message["body"],
      }
      if message.include?("errors")
        errors = message["errors"]
        reply_message["errors"] = errors unless errors.empty?
      end
      @forwarder.forward(reply_message, destination)
      $log.trace("#{log_tag}: reply: done")
    end

    private
    def log_tag
      "[#{Process.ppid}][#{Process.pid}] replier"
    end
  end
end
