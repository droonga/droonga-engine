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

require "groonga_command_converter"

class GroongaCommandConverterTest < Test::Unit::TestCase
  def setup
    options = {
      :id => "test",
      :date => date,
      :reply_to => reply_to,
      :status_code => status_code,
      :dataset => dataset,
    }
    @converter = Droonga::GroongaCommandConverter.new(options)
  end

  def test_table_create
    results = []
    command = "table_create Term TABLE_PAT_KEY ShortText " +
                "--default_tokenizer TokenBigram --normalizer NormalizerAuto"
    @converter.convert(command) do |droonga_command|
      results << droonga_command
    end
    assert_equal([
                   {
                     :id => "test:0",
                     :date => formatted_date,
                     :replyTo => reply_to,
                     :statusCode => status_code,
                     :dataset => dataset,
                     :type => "table_create",
                     :body => {
                       :name => "Term",
                       :flags => "TABLE_PAT_KEY",
                       :key_type => "ShortText",
                       :value_type => nil,
                       :default_tokenizer => "TokenBigram",
                       :normalizer => "NormalizerAuto",
                     },
                   },
                 ],
                 results)
  end

  private
  def date
    Time.new(2013, 11, 29, 0, 0, 0)
  end

  def formatted_date
    "2013-11-29T00:00:00+09:00"
  end

  def reply_to
    "localhost:20033"
  end

  def status_code
    200
  end

  def dataset
    "test-dataset"
  end
end
