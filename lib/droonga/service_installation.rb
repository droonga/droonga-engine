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

require "fileutils"

require "droonga/path"

module Droonga
  class ServiceInstallation
    class << self
    end

    class NotInstalledAsService < StandardError
    end

    def user_name
      "droonga-engine"
    end

    def group_name
      "droonga"
    end

    def base_directory
      @base_directory ||= Pathname("/home/#{user_name}/droonga")
    end

    def ensure_using_service_base_directory
      if user_exist?
        Path.base = base_directory.to_s
      end
    end

    def have_read_permission?
      test_file = Path.config
      begin
        test_file.read
      rescue Errno::EACCES => error
        return false
      end
      true
    end

    def have_write_permission?
      test_file = Path.base + "#{Time.now.to_i}.test"
      begin
        FileUtils.mkdir_p(Path.base)
        FileUtils.touch(test_file.to_s)
      rescue Errno::EACCES => error
      end
      unless test_file.exist?
        return false
      end
      FileUtils.rm_f(test_file.to_s)
      true
    end

    def user_exist?
      system("id", user_name,
             :out => "/dev/null",
             :err => "/dev/null")
    end

    def installed_as_service?
      return false unless user_exist?
    
      #TODO: we should support systemd also...
      succeeded = system("service", "droonga-engine", "status",
                         :out => "/dev/null",
                         :err => "/dev/null")
      return true if succeeded
    
      #TODO: we should support systemd also...
      result = `service droonga-engine status`
      result.include?("running") or \
        result.include?("droonga-engine is stopped") or \
        result.include?("droonga-engine dead")
    end

    def ensure_correct_file_permission(file)
      if user_exist?
        FileUtils.chown_R(user_name, group_name, file)
        FileUtils.chmod_R("g+r", file)
      end
    end

    def running?(pid_file_path=nil)
      raise NotInstalledAsService.new unless installed_as_service?
      #TODO: we should support systemd also...
      result = `service droonga-engine status`
      result.include?("is running")
    end

    def start
      raise NotInstalledAsService.new unless installed_as_service?
      #TODO: we should support systemd also...
      system("service", "droonga-engine", "start",
             :out => "/dev/null",
             :err => "/dev/null")
    end

    def stop
      raise NotInstalledAsService.new unless installed_as_service?
      #TODO: we should support systemd also...
      system("service", "droonga-engine", "stop",
             :out => "/dev/null",
             :err => "/dev/null")
    end
  end
end
