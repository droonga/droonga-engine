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

require "droonga/distributor_plugin"

module Droonga
  class WatchDistributor < Droonga::DistributorPlugin
    repository.register("watch", self)

    command "watch.feed" => :feed
    def feed(envelope)
      broadcast_all(envelope)
    end

    command "watch.subscribe" => :subscribe
    def subscribe(envelope)
      broadcast_all(envelope)
    end

    command "watch.unsubscribe" => :unsubscribe
    def unsubscribe(envelope)
      broadcast_all(envelope)
    end
  end
end
