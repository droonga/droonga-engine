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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

require "droonga/error"

module Droonga
  module Catalog
    class ValidationError < Error
      class Detail
        attr_reader :value_path, :message
        def initialize(value_path, message)
          @value_path = value_path
          @message = message
        end
      end

      attr_reader :path, :details
      def initialize(path, details)
        message = "validation error: <#{path}>"
        details.each do |detail|
          message << "\n  * #{detail.value_path}: #{detail.message}"
        end
        super(message)
      end
    end

    class LegacyValidationError < Error
      def initialize(message, path)
        if path
          super("[Validation Error <#{path}>]#{message}")
        else
          super(message)
        end
      end
    end

    class MissingRequiredParameter < LegacyValidationError
      def initialize(name, path)
        super("[#{name}] A required parameter is missing.", path)
      end
    end

    class MismatchedParameterType < LegacyValidationError
      def initialize(name, expected_types, actual, path)
        expected_types = [expected_types] unless expected_types.is_a?(Array)
        message = nil
        if expected_types.size == 1
          message = "[#{name}] Mismatched parameter type: " +
                      "expected=<#{expected_types.first}>, actual=<#{actual}>"
        else
          message = "[#{name}] Mismatched parameter type: " +
                      "expected=<#{expected_types.join(" or ")}>, " +
                      "actual=<#{actual}>"
        end
        super(message, path)
      end
    end

    class InvalidDate < LegacyValidationError
      def initialize(name, value, path)
        super("[#{name}] Invalid date string: <#{value}>", path)
      end
    end

    class NegativeNumber < LegacyValidationError
      def initialize(name, actual, path)
        super("[#{name}] A positive number is expected, but <#{actual}>", path)
      end
    end

    class SmallerThanOne < LegacyValidationError
      def initialize(name, actual, path)
        super("[#{name}] A number 1 or larger is expected, but <#{actual}>", path)
      end
    end

    class FarmNotZoned < LegacyValidationError
      def initialize(name, zones, path)
        super("The farm does not appear in zones: <#{name}>, zones=<#{zones}>", path)
      end
    end

    class UnknownFarmInZones < LegacyValidationError
      def initialize(name, zones, path)
        super("The farm is unknown: <#{name}>, zones=<#{zones}>", path)
      end
    end

    class UnknownFarmForPartition < LegacyValidationError
      def initialize(name, slice, path)
        super("The farm is unknown: <{#name}>, slice=<#{slice}>", path)
      end
    end

    class UnsupportedValue < LegacyValidationError
      def initialize(name, value, path)
        super("[#{name}] Not supported value: <#{value}>", path)
      end
    end
  end
end
