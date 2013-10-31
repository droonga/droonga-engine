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

module Droonga
  class Sweeper
    PERIODICAL_SWEEP_INTERVAL_SECONDS = 20 * 60 # 20 min
    SUBSCRIBER_LIFETIME_SECONDS = 10 * 60 # 10 min

    def initialize(context)
      @context = context
    end

    def activate_periodical_sleep(options={})
      interval = options[:interval] || PERIODICAL_SWEEP_INTERVAL_SECONDS
      @sweeper_thread = Thread.new do
        while true
          sweep_expired_subscribers
          sleep(interval)
        end
      end
    end

    def sweep_expired_subscribers
      boundary = Time.now - SUBSCRIBER_LIFETIME_SECONDS
      expired_subscribers = @context["Subscriber"].select do |subscriber|
        subscriber.last_modified < boundary
      end
      expired_subscribers.each do |subscriber|
        watcher.unsubscribe(:subscriber => subscriber._key)
      end
    end

    def watcher
      @watcher ||= Watcher.new(@context)
    end
  end
end
