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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

module Droonga
  class Sweeper
    SUBSCRIBER_LIFETIME_SECONDS = 10 * 60 # 10 min

    def initialize(context)
      @context = context
    end

    def sweep_expired_subscribers(options={})
      now = options[:now] || Time.now
      boundary = now - SUBSCRIBER_LIFETIME_SECONDS
      expired_subscribers = @context["Subscriber"].select do |subscriber|
        subscriber.last_modified < boundary
      end
      expired_subscribers.each do |subscriber|
        watcher.unsubscribe(:subscriber => subscriber._key)
      end
    end

    private
    def watcher
      @watcher ||= Watcher.new(@context)
    end
  end
end
