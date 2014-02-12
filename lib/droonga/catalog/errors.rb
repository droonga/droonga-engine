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

require "droonga/error"

module Droonga
  module Catalog
    class ValidationError < Error
      def initialize(message, path)
        if path
          super("Validation error in #{path}: #{message}")
        else
          super(message)
        end
      end
    end

    class MissingRequiredParameter < ValidationError
      def initialize(name, path)
        super("You must specify \"#{name}\".", path)
      end
    end

    class MismatchedParameterType < ValidationError
      def initialize(name, expected, actual, path)
        super("\"#{name}\" must be a #{expected}, but a #{actual}.", path)
      end
    end

    class InvalidDate < ValidationError
      def initialize(name, value, path)
        super("\"#{name}\" must be a valid datetime. " +
                "\"#{value}\" cannot be parsed as a datetime.", path)
      end
    end

    class NegativeNumber < ValidationError
      def initialize(name, actual, path)
        super("\"#{name}\" must be a positive number, but #{actual}.", path)
      end
    end

    class SmallerThanOne < ValidationError
      def initialize(name, actual, path)
        super("\"#{name}\" must be 1 or larger number, but #{actual}.", path)
      end
    end

    class UnknownFarm < ValidationError
      def initialize(name, partition, path)
        super("The partition #{partition} at \"#{name}\" seems to be bound to an unknown farm.", path)
      end
    end

    class UnsupportedValue < ValidationError
      def initialize(name, value, path)
        super("\"#{value}\" is not supported for \"#{name}\".", path)
      end
    end
  end
end
