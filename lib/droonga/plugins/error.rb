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

require "droonga/plugin"

module Droonga
  module Plugins
    module Error
      Plugin.registry.register("error", self)

      class Adapter < Droonga::Adapter
        output_message.pattern = ["body.errors", :exist]

        def adapt_output(output_message)
          errors = output_message.body["errors"]
          if errors and !errors.empty?
            output_message.errors = errors

            status_codes = []
            errors.values.each do |error|
              status_codes << error["statusCode"]
            end
            status_codes = status_codes.uniq
            if status_codes.size == 1
              output_message.status_code = status_codes.first
            else
              output_message.status_code = ErrorMessage::InternalServerError::STATUS_CODE
            end

            output_message.body = errors.values.first["body"]
          else
            output_message.body.delete("errors")
          end
        end
      end
    end
  end
end
