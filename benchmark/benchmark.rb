# -*- coding: utf-8 -*-
#
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

class Benchmark
  class Terms
    class << self
      def generate
        new.to_enum(:terms)
      end
    end

    FIRST_INITIAL_LETTER = "㐀"
    def terms
      initial_letter = FIRST_INITIAL_LETTER
      while true do
        yield "#{initial_letter}#{random_term}"
        initial_letter.succ!
      end
    end

    def random_term
      (("a".."z").to_a + ("A".."Z").to_a + (0..9).to_a).shuffle[0..7].join
    end
  end
end
