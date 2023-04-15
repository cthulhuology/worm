%% worm
%%
%% MIT No Attribution  
%% Copyright 2023 David J Goehrig <dave@dloh.org>
%%
%% Permission is hereby granted, free of charge, to any person obtaining a copy 
%% of this software and associated documentation files (the "Software"), to 
%% deal in the Software without restriction, including without limitation the 
%% rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
%% sell copies of the Software, and to permit persons to whom the Software is 
%% furnished to do so.  
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
%% IN THE SOFTWARE.

-module(worm).
-author({ "David J Goehrig", "dave@dloh.org"}).
-copyright(<<"© 2023 David J. Goehrig"/utf8>>).
-export([ init/1, write/3, find/2, sync/1, delete/1, reindex/1 ]).

-record(worm,{ name, index_file, index, data_file, data, data_fp }).

init(Directory) ->
	ok = make_directory(Directory),
	ok = make_directory(Directory ++ "/views"),
	IndexFile = Directory ++ "/index",
	Index = load_index(IndexFile),
	DataFile = Directory ++ "/data", 
	Data = load_data(DataFile),
	DataFP = make_file(Directory,"data"),
	#worm{ name=Directory, 
		index=Index, index_file=IndexFile, 
		data=Data, data_file=DataFile, data_fp = DataFP }.

delete(#worm{ name = Directory }) ->
	file:del_dir_r(Directory).	

make_directory(Directory) ->
	case file:make_dir(Directory) of
		ok -> ok;
		{error, eexist} -> ok;
		Error -> Error
	end.

make_file(Directory, Filename) ->
	FilePath = filename:join(Directory, Filename),
	case file:open(FilePath, [binary, append]) of
		{ok, FileHandle} -> FileHandle;
		{error, enoent} ->
			{ok, FileHandle} = file:open(FilePath, [write, binary]),
			FileHandle,
			file:close(FileHandle),
			make_file(Directory,Filename);
		Error -> Error
	end.

load_index(IndexFile) ->
	case file:read_file(IndexFile) of 
		{ok, Bin} ->
			erlang:binary_to_term(zlib:unzip(Bin));
		_ ->
			gb_trees:empty()
	end.

load_data(DataFile) ->
	case file:read_file(DataFile) of
		{ok, Bin} ->
			Bin;
		{error, enoent } ->
			<<>>;
		{error, Reason } ->
			error_logger:error_msg("Failed to load data ~p: ~p~n", [ DataFile, Reason ]),
			<<>>
	end.

reindex(<<>>,_Offset,Index) ->
	Index;
reindex(Bin = <<Len:32/little-unsigned-integer,_:Len/binary,IdLen:8,Id:IdLen/binary,Rest/binary>>,Offset,Index) ->
	EntryLen = byte_size(Bin) - byte_size(Rest),
	NewIndex = gb_trees:enter(Id,Offset,Index),
	reindex(Rest,Offset+EntryLen,NewIndex).

reindex(Worm = #worm{ data_file=DataFile }) ->
	Data = load_data(DataFile),
	Index = gb_trees:empty(),
	NewIndex = reindex(Data,0,Index),
	sync(Worm#worm{ data = Data, index = NewIndex}).


sync(Worm =  #worm{ index_file=IndexFile, index=Index }) ->
	Bin = zlib:zip(erlang:term_to_binary(gb_trees:balance(Index))),
	case file:write_file(IndexFile,Bin) of
		ok -> {ok, Worm };
		{ error, Reason } ->
			error_logger:error_msg("Error writing index ~p: ~p~n",[ IndexFile, Reason ]),
			{ error, Reason, Worm }
	end.

write(Worm = #worm{ index=Index, data=Data, data_fp=DataFP },Id,Object) ->
	IdLen = byte_size(Id),
	Len = byte_size(Object),
	DataLen = byte_size(Data),
	NewIndex = gb_trees:enter(Id,DataLen,Index),
	NewData = <<Data/binary,Len:32/little-unsigned-integer,Object/binary,IdLen:8,Id/binary>>,
	ok = file:write(DataFP,<<Len:32/little-unsigned-integer,Object/binary,IdLen:8,Id/binary>>),
	{ ok, Worm#worm{ index=NewIndex, data=NewData } }.

find(#worm{ index=Index, data=Data }, Id) ->
	case gb_trees:lookup(Id,Index) of
	{ value, Result } ->
		<<Len:32/little-unsigned-integer>> = binary:part(Data,{ Result, 4} ),
		<<Object:Len/binary>> = binary:part(Data, { Result + 4, Len }),
		{ok, Object };
	none ->
		none
	end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

init_test() ->
	delete(#worm{ name="test"}),
	Tree=gb_trees:empty(),
	?assertMatch(#worm{ name="test", index_file="test/index", data_file="test/data", data= <<>>, data_fp=_, index=Tree}, init("test")),
	delete(#worm{ name="test"}).


write_test() ->
	delete(#worm{ name="test"}),
	Worm = init("test"),
	UUID1 = uuid:new(),
	Object1 = <<"This is a test of the index writing">>,
	UUID2 = uuid:new(),
	Object2 = <<"This is only a test">>,
	?assertMatch({ ok, #worm{ name="test", index_file="test/index", data_file="test/data", data=_, data_fp=_, index=_}}, write(Worm,UUID1,Object1)),
	?assertMatch({ ok, #worm{ name="test", index_file="test/index", data_file="test/data", data=_, data_fp=_, index=_}}, write(Worm,UUID2,Object2)),
	delete(#worm{ name="test"}).
	
find_test() ->
	delete(#worm{ name="test"}),
	Worm = init("test"),
	UUID1 = uuid:new(),
	Object1 = <<"This is a test of the index writing">>,
	UUID2 = uuid:new(),
	Object2 = <<"This is only a test">>,
	{ ok, Worm1} = write(Worm,UUID1,Object1),
	{ ok, Worm2} = write(Worm1,UUID2,Object2),

	?assertEqual({ ok, Object2 },find(Worm2,UUID2)),
	?assertEqual({ ok, Object1 },find(Worm2,UUID1)),

	delete(#worm{ name="test"}).
	

sync_test() ->
	delete(#worm{ name="test"}),
	Worm = init("test"),
	UUID1 = uuid:new(),
	Object1 = <<"This is a test of the index writing">>,
	UUID2 = uuid:new(),
	Object2 = <<"This is only a test">>,
	{ ok, Worm1} = write(Worm,UUID1,Object1),
	{ ok, Worm2} = write(Worm1,UUID2,Object2),
	sync(Worm2),
	Worm3 = init("test"),
	?assertEqual({ ok, Object1 },find(Worm3,UUID1)),
	delete(#worm{ name="test"}).

reindex_test() ->
	delete(#worm{ name="test"}),
	Worm = init("test"),
	UUID1 = uuid:new(),
	Object1 = <<"This is a test of the index writing">>,
	UUID2 = uuid:new(),
	Object2 = <<"This is only a test">>,
	{ ok, Worm1} = write(Worm,UUID1,Object1),
	{ ok, Worm2} = write(Worm1,UUID2,Object2),
	%% NB we never synced the index to disk here so we aren't loading from disk
	Worm3 = init("test"),
	{ ok, Worm4 } = reindex(Worm3),
	?assertEqual(none, find(Worm3,UUID1)),
	?assertEqual({ ok, Object1 },find(Worm4,UUID1)),
	?assertEqual(Worm4#worm.index, Worm2#worm.index),
	delete(#worm{ name="test"}).

-endif.
