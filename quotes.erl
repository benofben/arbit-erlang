-module(quotes).

-record(quote, {key, symbol, timestamp, open, high, low, close, volume}).

-export([main/0, do/1, listSymbols/0, processQuoteFile/1, processLines/1]).

-include_lib("stdlib/include/qlc.hrl").

-compile(export_all).

main() ->
	mnesia:create_schema([node()]),
	mnesia:start(),
	mnesia:create_table(quote, [{attributes, record_info(fields, quote)}]),
	mnesia:stop(),
	
	mnesia:start(),
	mnesia:wait_for_tables([quote], 20000),
	
	Filename="C:\\arbitData\\ameritrade\\quotes\\A\\20100325.csv",
	processQuoteFile(Filename),
		
	X=do(qlc:q([X || X <- mnesia:table(quote)])),
	erlang:display(X),
	
	mnesia:stop().

do(Q) ->
	F = fun() -> qlc:e(Q) end,
	{atomic,Val} = mnesia:transaction(F),
	Val.

listSymbols() ->
	Directory="C:\\arbitData\\ameritrade\\quotes\\",
	case file:list_dir(Directory) of
		{ok, Symbols} ->
			Symbols;
		{error, Reason} ->
			Reason
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
