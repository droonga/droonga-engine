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

require "droonga/catalog/errors"

module Droonga
  module Catalog
    class Version2Validator
      def initialize(data, path)
        @data = data
        @path = path
      end

      def validate
        @details = []

        validate_datasets

        unless @details.empty?
          raise ValidationError.new(@path, @details)
        end
      end

      private
      def validate_datasets
        unless @data.key?("datasets")
          required_parameter_is_missing("datasets")
          return
        end
        @data["datasets"].each do |name, dataset|
          validate_dataset(name, dataset)
        end
      end

      def validate_dataset(name, dataset)
        unless dataset.key?("replicas")
          required_parameter_is_missing("datasets.#{name}.replicas")
          return
        end
      end

      def add_detail(value_path, message)
        @details << ValidationError::Detail.new(value_path, message)
      end

      def required_parameter_is_missing(value_path)
        add_detail(value_path, "required parameter is missing")
      end
    end
  end
end
