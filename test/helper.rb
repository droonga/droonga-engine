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

module Sandbox
  class << self
    def included(base)
      base.setup :setup_sandbox, :before => :prepend
      base.teardown :teardown_sandbox, :after => :append
    end
  end

  def setup_sandbox
    setup_temporary_directory

    setup_context

    @database_path = @temporary_directory + "database"
    @database = nil
  end

  def setup_temporary_directory
    @base_temporary_directory = Pathname(File.dirname(__FILE__)) + "tmp"
    memory_file_system = "/run/shm"
    if File.exist?(memory_file_system)
      FileUtils.mkdir_p(@base_temporary_directory.parent.to_s)
      FileUtils.rm_f(@base_temporary_directory.to_s)
      FileUtils.ln_s(memory_file_system, @base_temporary_directory.to_s)
    else
      FileUtils.mkdir_p(@base_temporary_directory.to_s)
    end

    @temporary_directory = @base_temporary_directory + "fluent-plugin-droonga"
    FileUtils.rm_rf(@temporary_directory.to_s)
    FileUtils.mkdir_p(@temporary_directory.to_s)
  end

  def setup_context
    Groonga::Context.default = nil
    Groonga::Context.default_options = nil
  end

  def restore(dumped_command)
    context = Groonga::Context.new
    database = context.create_database(@database_path.to_s)
    context.restore(dumped_command)
    database.close
    context.close
  end

  def teardown_sandbox
    Groonga::Context.default.close
    Groonga::Context.default = nil
    GC.start
    teardown_temporary_directory
  end

  def teardown_temporary_directory
    FileUtils.rm_rf(@temporary_directory.to_s)
    FileUtils.rm_rf(@base_temporary_directory.to_s)
  end
end

module Fixture
  def fixture_directory
    File.join(File.dirname(__FILE__), "fixtures")
  end

  def fixture_path(*path_components)
    File.join(fixture_directory, *path_components)
  end

  def fixture_data(*path_components)
    File.read(fixture_path(*path_components))
  end
end

class Test::Unit::TestCase
  include Sandbox
  include Fixture
end
