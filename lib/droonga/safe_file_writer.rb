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
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

require "pathname"
require "fileutils"
require "tempfile"

module Droonga
  class SafeFileWriter
    class << self
      def write(path, contents=nil)
        # Don't output the file directly to prevent loading of incomplete file!
        path = Pathname(path).expand_path
        FileUtils.mkdir_p(path.dirname.to_s)
        Tempfile.open(path.basename.to_s, path.dirname.to_s, "w") do |output|
          if block_given?
            yield(output)
          else
            output.write(contents)
          end
          output.flush
          File.rename(output.path, path.to_s)
        end
      end
    end
  end
end
