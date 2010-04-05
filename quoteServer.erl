-module(quoteServer).
-export([start/0]).
-record(quote, {key, symbol, timestamp, open, high, low, close, volume}).

start() ->
	erlang:display("quoteServer: Starting quote server..."),
	
	mnesia:start(),
	mnesia:create_table(quote, [{attributes, record_info(fields, quote)}]),
	mnesia:wait_for_tables([quote], 1000),

	Symbols=listSymbols(),
	%Symbols=["A","AA"],
	processSymbols(Symbols),

	erlang:display("quoteServer: Done loading.").

listSymbols() ->
	Directory="/home/ben/arbitData/ameritrade/quotes/",
	case file:list_dir(Directory) of
		{ok, Symbols} ->
			Symbols
	end.

processSymbols(Symbols) ->
	case Symbols of
		[]->ok;
		Symbols ->
			[Symbol|SymbolsSublist]=Symbols,			
			processSymbol(Symbol),
			processSymbols(SymbolsSublist)
	end.

processSymbol(Symbol) ->
	Wildcard=lists:concat(["/home/ben/arbitData/ameritrade/quotes/", Symbol, "/*.*"]),
	Filenames=filelib:wildcard(Wildcard),
	processQuoteFiles(Filenames).

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
