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

require "groonga_command_converter"

class GroongaCommandConverterTest < Test::Unit::TestCase
  def setup
    options = {
      :id => "test",
      :date => date,
      :reply_to => reply_to,
      :dataset => dataset,
    }
    @converter = Droonga::GroongaCommandConverter.new(options)
  end

  def test_table_create
    droonga_commands = []
    command = <<-COMMAND.chomp
table_create Terms TABLE_PAT_KEY ShortText \
  --default_tokenizer TokenBigram --normalizer NormalizerAuto
    COMMAND
    @converter.convert(command) do |droonga_command|
      droonga_commands << droonga_command
    end
    assert_equal([
                   {
                     :id => "test:0",
                     :date => formatted_date,
                     :replyTo => reply_to,
                     :dataset => dataset,
                     :type => "table_create",
                     :body => {
                       :name => "Terms",
                       :flags => "TABLE_PAT_KEY",
                       :key_type => "ShortText",
                       :default_tokenizer => "TokenBigram",
                       :normalizer => "NormalizerAuto",
                     },
                   },
                 ],
                 droonga_commands)
  end

  def test_column_create
    droonga_commands = []
    command = <<-COMMAND.chomp
column_create Terms Users_name COLUMN_INDEX|WITH_POSITION Users name
    COMMAND
    @converter.convert(command) do |droonga_command|
      droonga_commands << droonga_command
    end
    assert_equal([
                   {
                     :id => "test:0",
                     :date => formatted_date,
                     :replyTo => reply_to,
                     :dataset => dataset,
                     :type => "column_create",
                     :body => {
                       :table => "Terms",
                       :name => "Users_name",
                       :flags => "COLUMN_INDEX|WITH_POSITION",
                       :type => "Users",
                       :source => "name",
                     },
                   },
                 ],
                 droonga_commands)
  end

  def test_load
    droonga_commands = []
    command = <<-COMMAND.chomp
load --table Users
[
["_key","name"],
["user0","Abe Shinzo"],
["user1","Noda Yoshihiko"],
["user2","Kan Naoto"]
]
    COMMAND
    @converter.convert(command) do |droonga_command|
      droonga_commands << droonga_command
    end
    assert_equal([
                   {
                     :id => "test:0",
                     :date => formatted_date,
                     :replyTo => reply_to,
                     :dataset => dataset,
                     :type => "add",
                     :body => {
                       :table => "Users",
                       :key => "user0",
                       :values => {
                         :name => "Abe Shinzo",
                       },
                     },
                   },
                   {
                     :id => "test:1",
                     :date => formatted_date,
                     :replyTo => reply_to,
                     :dataset => dataset,
                     :type => "add",
                     :body => {
                       :table => "Users",
                       :key => "user1",
                       :values => {
                         :name => "Noda Yoshihiko",
                       },
                     },
                   },
                   {
                     :id => "test:2",
                     :date => formatted_date,
                     :replyTo => reply_to,
                     :dataset => dataset,
                     :type => "add",
                     :body => {
                       :table => "Users",
                       :key => "user2",
                       :values => {
                         :name => "Kan Naoto",
                       },
                     },
                   },
                 ],
                 droonga_commands)
  end

  def test_select
    droonga_commands = []
    command = <<-COMMAND.chomp
select --filter "age<=30" --output_type "json" --table "Users"
    COMMAND
    @converter.convert(command) do |droonga_command|
      droonga_commands << droonga_command
    end
    assert_equal([
                   {
                     :id => "test:0",
                     :date => formatted_date,
                     :replyTo => reply_to,
                     :dataset => dataset,
                     :type => "select",
                     :body => {
                       :table => "Users",
                       :filter => "age<=30",
                       :output_type => "json",
                     },
                   },
                 ],
                 droonga_commands)
  end

  def test_multiple_commands
    droonga_commands = []
    commands = <<-COMMANDS.chomp
table_create Terms TABLE_PAT_KEY ShortText \
  --default_tokenizer TokenBigram --normalizer NormalizerAuto
column_create Terms Users_name COLUMN_INDEX|WITH_POSITION Users name
    COMMANDS
    @converter.convert(commands) do |droonga_command|
      droonga_commands << droonga_command
    end
    assert_equal([
                   {
                     :id => "test:0",
                     :date => formatted_date,
                     :replyTo => reply_to,
                     :dataset => dataset,
                     :type => "table_create",
                     :body => {
                       :name => "Terms",
                       :flags => "TABLE_PAT_KEY",
                       :key_type => "ShortText",
                       :default_tokenizer => "TokenBigram",
                       :normalizer => "NormalizerAuto",
                     },
                   },
                   {
                     :id => "test:1",
                     :date => formatted_date,
                     :replyTo => reply_to,
                     :dataset => dataset,
                     :type => "column_create",
                     :body => {
                       :table => "Terms",
                       :name => "Users_name",
                       :flags => "COLUMN_INDEX|WITH_POSITION",
                       :type => "Users",
                       :source => "name",
                     },
                   },
                 ],
                 droonga_commands)
  end

  private
  def date
    Time.utc(2013, 11, 29, 0, 0, 0)
  end

  def formatted_date
    "2013-11-29T00:00:00Z"
  end

  def reply_to
    "localhost:20033"
  end

  def dataset
    "test-dataset"
  end
end
