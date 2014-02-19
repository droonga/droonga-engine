# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Droonga Project
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
  class Error < StandardError
  end

  class MultiplexError < Error
    attr_reader :errors

    def initialize(errors=[])
      @errors = errors
      error_messages = @errors.collect do |error|
        error.message
      end
      message = error_messages.sort.join("\n-----------------------\n")
      super(message)
    end
  end

  # the base class for any error which can be described as a Droonga message
  class ErrorMessage < Error
    STATUS_CODE = nil

    attr_reader :detail

    def initialize(message, detail=nil)
      @detail = detail
      super(message)
    end

    def name
      self.class.name.split("::").last
    end

    def status_code
      self.class::STATUS_CODE
    end

    def response_body
      body = {
        "name"    => name,
        "message" => message,
      }
      body["detail"] = @detail unless @detail.nil?
      body
    end
  end

  # TODO: Move to common file for runners
  class UnsupportedMessageError < Error
    attr_reader :phase, :message
    def initialize(phase, message)
      @phase = phase
      @message = message
      super("[#{@phase}] Unsupported message: #{@message.inspect}")
    end
  end
end
