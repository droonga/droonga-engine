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

    class MissingPidFilePath < ArgumentError
    end

    def user_name
      "droonga-engine"
    end

    def group_name
      user_name
    end

    def base_directory
      @base_directory ||= Pathname("/home/#{user_name}/droonga")
    end

    def ensure_using_service_base_directory
      if user_exist?
        Path.base = base_directory
      end
    end

    def have_write_permission?
      test_file = base_directory + "#{Time.now.to_i}.test"
      begin
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
    
      succeeded = system("service", "droonga-engine", "status",
                         :out => "/dev/null",
                         :err => "/dev/null")
      return true if succeeded
    
      result = `env SYSTEMCTL_SKIP_REDIRECT=yes service droonga-engine status`
      result.include?("running") or \
        result.include?("droonga-engine is stopped")
    end

    def ensure_correct_file_permission(file)
      if user_exist?
        FileUtils.chown(user_name, group_name, file)
      end
    end

    def running?(pid_file_path=nil)
      if installed_as_service?
        result = `env SYSTEMCTL_SKIP_REDIRECT=yes service droonga-engine status`
        result.include?("running")
      else
        if pid_file_path.nil?
          raise MissingPidFilePath.new
        end
        system("droonga-engine-status",
               "--base-dir", Path.base.to_s,
               "--pid-file", pid_file_path.to_s,
               :out => "/dev/null",
               :err => "/dev/null")
      end
    end

    def start
      if installed_as_service?
        system("service", "droonga-engine", "start",
               :out => "/dev/null",
               :err => "/dev/null")
      else
        false
      end
    end

    def stop(pid_file_path=nil)
      if installed_as_service?
        system("service", "droonga-engine", "stop",
               :out => "/dev/null",
               :err => "/dev/null")
      else
        if pid_file_path.nil?
          raise MissingPidFilePath.new
        end
        system("droonga-engine-stop",
               "--base-dir", Path.base.to_s,
               "--pid-file", pid_file_path.to_s,
               :out => "/dev/null",
               :err => "/dev/null")
      end
    end
  end
end
