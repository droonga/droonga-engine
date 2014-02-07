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

require "droonga/plugin/metadata/plugin"

module Droonga
  module Pluggable
    def plugin
      Plugin::Metadata::Plugin.new(self)
    end

    def options
      @options ||= {}
    end

    def find_sub_classes(names)
      target_sub_classes = []
      names.each do |name|
        sub_classes = Plugin.registry.find_sub_classes(name, self)
        target_sub_classes.concat(sub_classes)
      end
      target_sub_classes
    end
  end
end
