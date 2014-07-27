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

require "droonga/catalog/version2_validator"

class CatalogVersion2ValidatorTest < Test::Unit::TestCase
  def setup
    @path = "catalog.json"
  end

  def validate(data)
    validator = Droonga::Catalog::Version2Validator.new(data, @path)
    validator.validate
  end

  def validation_error(details)
    Droonga::Catalog::ValidationError.new(@path, details)
  end

  def detail(value_path, message)
    Droonga::Catalog::ValidationError::Detail.new(value_path, message)
  end

  class DatasetsTest < self
    def test_missing
      details = [
        detail("datasets", "required parameter is missing"),
      ]
      assert_raise(validation_error(details)) do
        validate({})
      end
    end

    class DatasetTest < self
      class ReplicasTest < self
        def test_missing
          details = [
            detail("datasets.Droonga.replicas",
                   "required parameter is missing"),
          ]
          assert_raise(validation_error(details)) do
            data = {
              "datasets" => {
                "Droonga" => {
                }
              }
            }
            validate(data)
          end
        end
      end
    end
  end
end
