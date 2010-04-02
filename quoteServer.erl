-module(quoteServer).
-export([start/1]).
-record(quote, {key, symbol, timestamp, open, high, low, close, volume}).

start(Symbol) ->
	erlang:display(lists:concat(["quoteServer ", Symbol, ": Starting quote server."])),

	mnesia:start(),
	mnesia:create_table(quote, [{attributes, record_info(fields, quote)}]),
	mnesia:wait_for_tables([quote], 1000),

	Wildcard=lists:concat(["C:/arbitData/ameritrade/quotes/", Symbol, "/*.*"]),
	Filenames=filelib:wildcard(Wildcard),
	processQuoteFiles(Filenames),

	erlang:display(lists:concat(["quoteServer ", Symbol, ": Done loading."])),
	loop(Symbol).

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
		RawLine ->
			[Line]=string:tokens(RawLine, [$\n]),
			[Symbol, TimestampString, OpenString, HighString, LowString, CloseString, VolumeString]=string:tokens(Line, [$,]),
			Key=lists:concat([Symbol, TimestampString]),
			Timestamp=list_to_integer(TimestampString),
			Open=list_to_float(OpenString),
			High=list_to_float(HighString),
			Low=list_to_float(LowString),
			Close=list_to_float(CloseString),
			Volume=list_to_integer(VolumeString),
			
			Row=#quote{key=Key, timestamp=Timestamp, symbol=Symbol,  open=Open, high=High, low=Low, close=Close, volume=Volume},
			F=fun() ->
					  mnesia:write(Row)
			  end,
			mnesia:transaction(F),
			processLines(IODevice)
	end.

loop (Symbol) ->
	receive
		Other ->
			erlang:display(lists:concat(["quoteServer ", Symbol, ": Unknown request."])),
			erlang:display(lists:concat(["quoteServer ", Symbol, ": ", Other])),
			loop(Symbol)
	end.
