# -*- coding: utf-8 -*-
#
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

require "MeCab"

module Droonga
  class Searcher
    class QuerySearcher
      class MeCabTokenizer
        def initialize
          @mecab = MeCab::Tagger.new("-Owakati")
        end

        def tokenize(text)
          tokens = @mecab.parse(text).force_encoding("utf-8").split(/\s+/)
          tokens.reject do |token|
            token.empty?
          end
        end
      end

      def apply_mecab_filter(condition)
        return unless condition.is_a?(Hash)
        return unless condition["useMeCabFilter"]
        query = condition["query"]
        return if query.nil?
        match_columns = condition["matchTo"]
        return unless match_columns.is_a?(Array)
        return if match_columns.size != 1
        match_column = match_columns.first

        tokenizer = MeCabTokenizer.new

        @records.open_cursor do |cursor|
          count = 0
          cursor.each do |record|
            match_target = record[match_column]
            body_terms = tokenizer.tokenize(match_target)
            unless body_terms.include?(query)
              record.delete
            end
          end
        end
      end

      alias_method :original_apply_condition!, :apply_condition!
      def apply_condition!(condition)
        original_apply_condition!(condition)
        apply_mecab_filter(condition)
      end
    end
  end
end
