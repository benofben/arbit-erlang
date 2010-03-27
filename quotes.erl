-module(quotes).
-record(quote, {key, symbol, timestamp, open, high, low, close, volume}).
-export([start/0, stop/0, do/1, processQuoteFiles/1, processQuoteFile/1, processLines/1]).
-include_lib("stdlib/include/qlc.hrl").
-compile(export_all).

start() ->
	mnesia:create_schema([node()]),
	mnesia:start(),
	mnesia:create_table(quote, [{attributes, record_info(fields, quote)}]),
	mnesia:wait_for_tables([quote], 20000),
	
	Directory="C:/arbitData/ameritrade/quotes/",
	Filenames=filelib:wildcard(lists:concat([Directory, "A/*.*"])),
	processQuoteFiles(Filenames).
		
	%X=do(qlc:q([X || X <- mnesia:table(quote)])).
	%erlang:display(X).
	
stop() ->
	mnesia:stop().

do(Q) ->
	F = fun() -> qlc:e(Q) end,
	{atomic,Val} = mnesia:transaction(F),
	Val.

processQuoteFiles(Filenames) ->
	case Filenames of
		[] -> ok;
		Filenames ->
			[Filename|FilenamesSublist]=Filenames,
			erlang:display(lists:concat(["Loading ", Filename])),
			processQuoteFile(Filename),
			processQuoteFiles(FilenamesSublist)
	end.

processQuoteFile(Filename) ->
	case file:open(Filename, read) of
		{ok, IODevice} ->
			processLines(IODevice),
			file:close(IODevice);
		Error -> 
			Error
	end.

processLines(IODevice) ->
	case io:get_line(IODevice, '') of
		eof -> ok;
		Line -> 
			[Symbol, Timestamp, Open, High, Low, Close, Volume]=string:tokens(Line, [$,]),
			Key=lists:concat([Symbol, Timestamp]),
			Row=#quote{key=Key, symbol=Symbol, timestamp=Timestamp, open=Open, high=High, low=Low, close=Close, volume=Volume},
			F=fun() ->
					  mnesia:write(Row)
			  end,
			mnesia:transaction(F),
			processLines(IODevice)
	end.
