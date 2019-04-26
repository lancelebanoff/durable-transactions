# ACID Transactional Data Structures

## Organization

`src/lockfreelist`: The code for a lock-free linked list, taken from the LFTT repository. Based on Tim Harris' lock-free linked list.

`src/boostinglist`: The code for a lock-free linked list supporting Isolation transactions, taken from the LFTT repository. Based on Transactional Boosting, developed by Maurice Herlihy and Eric Koskinen.

`src/durabletxn`: Work-in-progress. A rudimentary library to support Durable transactions, based on undo logging.

Using both the Isolation and Durable transactions in conjunction will yield ACID transactions.

## Build Instructions

To compile and run:
	make
	./a.out

To debug:
	make d
	gdb ./a.out
