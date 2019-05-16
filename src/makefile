
default: main.cc lockfreelist/lockfreelist.cc lockfreelist/lockfreelist.h boostinglist/boostinglist.cc boostinglist/boostinglist.h boostinglist/lockkey.cc boostinglist/lockkey.h durabletxn/dtx.cc durabletxn/dtx.h
	g++ main.cc lockfreelist/lockfreelist.cc lockfreelist/lockfreelist.h boostinglist/boostinglist.cc boostinglist/boostinglist.h boostinglist/lockkey.cc boostinglist/lockkey.h durabletxn/dtx.cc durabletxn/dtx.h common/allocator.cc common/timehelper.cc common/assert.cc common/allocator.h common/timehelper.h common/assert.h lftt/translist.cc lftt/translist.h logging.h -pthread -std=c++11 -lrt -ltbb -ltcmalloc


d: main.cc lockfreelist/lockfreelist.cc lockfreelist/lockfreelist.h boostinglist/boostinglist.cc boostinglist/boostinglist.h boostinglist/lockkey.cc boostinglist/lockkey.h durabletxn/dtx.cc durabletxn/dtx.h
	g++ main.cc lockfreelist/lockfreelist.cc lockfreelist/lockfreelist.h boostinglist/boostinglist.cc boostinglist/boostinglist.h boostinglist/lockkey.cc boostinglist/lockkey.h durabletxn/dtx.cc durabletxn/dtx.h -pthread -std=c++11 -ltbb -ltcmalloc -g