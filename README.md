worm - write one read many for erlang
-------------------------------------

This is a simple module for logging data to a file w/ index.  

The index file is just a zipped gb_tree which is balanced and
written to disk by the worm:sync/1 function.  The log file is
is of the format

	+-------+-------------+---+--------+
	| Len   | Object[Len] | B | Id[B]  |
	+-------+-------------+---+--------+

Where Objects are just binaries, and the Id is usually a UUID
(but doesn't have to be, just a binary of 255 bytes or less.

The worm:reindex/1 function can be used to rebuild the index
by scanning the whole file linearly.  This is useful if the 
index is corrupted or lost, say due to a program crash. If
the last write fails, the reindex will fail currently, and
you might have to prune some data.  No effort has been put
into ensuring the data  integrity at this point, so you get
what you get.

The live index and a copy of the data file are currently kept
in memory for fast lookups.  In the future it would probably
make sense to have a plan for rotating worms based on in
memory size, but that's really something that would be 
application dependent.  A server might for example want to
log the index every minute or every hour, and then rotate.



MIT License

Copyright (c) 2023 David J Goehrig

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 
