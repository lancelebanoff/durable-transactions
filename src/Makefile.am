
bin_PROGRAMS = trans

SUBDIRS = common/fraser

trans_SOURCES = bench/main.cc \
				common/allocator.cc\
				common/timehelper.cc\
				common/assert.cc\
				lftt/list/translist.cc\
				lftt/skiplist/transskip.cc\
				durabletxn/dtx.cc\
				boostinglist/boostinglist.cc\
				lockfreelist/lockfreelist.cc\
				boostinglist/lockkey.cc

trans_CPPFLAGS = $(WARN_FLAGS) -std=c++11 -pthread\
				 -DINTEL -DCACHE_LINE_SIZE=`getconf LEVEL1_DCACHE_LINESIZE`\
				 -I$(top_builddir)/src -I$(top_srcdir)/src

trans_LDADD = -lrt -ltbb -ltcmalloc common/fraser/libfd.la
