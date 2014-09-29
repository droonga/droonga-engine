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

require "stringio"
require "tmpdir"
require "fileutils"

require "faraday"
require "faraday_middleware"
require "archive/zip"

require "droonga/loggable"

module Droonga
  class SerfDownloader
    include Loggable

    class DownloadFailed < StandardError
    end

    MAX_RETRY_COUNT = 5
    RETRY_INTERVAL  = 10

    def initialize(output_path)
      @output_path = output_path
      @retry_count = 0
    end

    def download
      detect_platform
      version = "0.6.3"
      url_base = "https://dl.bintray.com/mitchellh/serf"
      base_name = "#{version}_#{@os}_#{@architecture}.zip"
      connection = Faraday.new(url_base) do |builder|
        builder.response(:follow_redirects)
        builder.adapter(Faraday.default_adapter)
      end
      response = connection.get(base_name)
      absolete_output_path = @output_path.expand_path
      Dir.mktmpdir do |dir|
        Archive::Zip.extract(StringIO.new(response.body),
                             dir,
                             :directories => false)
        FileUtils.mv("#{dir}/serf", absolete_output_path.to_s)
        FileUtils.chmod(0755, absolete_output_path.to_s)
      end
    rescue Archive::Zip::UnzipError => archive_error
      logger.warn("Downloaded zip file is broken.")
      if @retry_count < MAX_RETRY_COUNT
        @retry_count += 1
        sleep(RETRY_INTERVAL)
        download
      else
        raise DownloadFailed.new("Couldn't download serf executable. Try it later.")
      end
    rescue Faraday::ConnectionFailed => network_error
      logger.warn("Cinnection failed.")
      if @retry_count < MAX_RETRY_COUNT
        @retry_count += 1
        sleep(RETRY_INTERVAL)
        download
      else
        raise DownloadFailed.new("Couldn't download serf executable. Try it later.")
      end
    end

    private
    def detect_platform
      detect_os
      detect_architecture
    end

    def detect_os
      case RUBY_PLATFORM
      when /linux/
        @os = "linux"
      when /freebsd/
        @os = "freebsd"
      when /darwin/
        @os = "darwin"
      when /mswin|mingw/
        @os = "windows"
      else
        raise "Unsupported OS: #{RUBY_PLATFORM}"
      end
    end

    def detect_architecture
      case RUBY_PLATFORM
      when /x86_64|x64/
        @architecture = "amd64"
      when /i\d86/
        @architecture = "386"
      else
        raise "Unsupported architecture: #{RUBY_PLATFORM}"
      end
    end

    def log_tag
      "serf-downloader"
    end
  end
end
