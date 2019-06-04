
bin_PROGRAMS = trans


trans_SOURCES = bench/main.cc \
				common/allocator.cc\
				common/timehelper.cc\
				common/assert.cc\
				lftt/list/translist.cc\
				durabletxn/dtx.cc\
				boostinglist/boostinglist.cc\
				lockfreelist/lockfreelist.cc\
				boostinglist/lockkey.cc

trans_CPPFLAGS = $(WARN_FLAGS) -std=c++11 -pthread\
				 -I$(top_builddir)/src -I$(top_srcdir)/src

trans_LDADD = -lrt -ltbb -ltcmalloc
