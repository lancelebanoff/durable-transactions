#
# To compile:
#	make
#
# To run:
#	./a.out
#

default: main.cc lockfreelist/lockfreelist.cc lockfreelist/lockfreelist.h boostinglist/boostinglist.cc boostinglist/boostinglist.h boostinglist/lockkey.cc boostinglist/lockkey.h
	g++ main.cc lockfreelist/lockfreelist.cc lockfreelist/lockfreelist.h boostinglist/boostinglist.cc boostinglist/boostinglist.h boostinglist/lockkey.cc boostinglist/lockkey.h -pthread -std=c++11 -ltbb

d: main.cc lockfreelist/lockfreelist.cc lockfreelist/lockfreelist.h boostinglist/boostinglist.cc boostinglist/boostinglist.h boostinglist/lockkey.cc boostinglist/lockkey.h
	g++ main.cc lockfreelist/lockfreelist.cc lockfreelist/lockfreelist.h boostinglist/boostinglist.cc boostinglist/boostinglist.h boostinglist/lockkey.cc boostinglist/lockkey.h -pthread -std=c++11 -ltbb -g