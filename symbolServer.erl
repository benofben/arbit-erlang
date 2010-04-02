-module(symbolServer).
-export([start/0]).
-record(symbol, {symbol, x}).

start() ->
	erlang:display("symbolServer: Starting symbol server..."),
	erlang:display("symbolServer: I will start the quote servers now."),
	
	mnesia:start(),
	mnesia:create_table(symbol, [{attributes, record_info(fields, symbol)}]),
	mnesia:wait_for_tables([symbol], 1000),

	Symbols=listSymbols(),
	%Symbols=["A","AA"],
	processSymbols(Symbols),

	erlang:display("symbolServer: Done loading."),
	loop().

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
			Row=#symbol{symbol=Symbol, x=0},
			F=fun() ->
					  mnesia:write(Row)
			  end,
			mnesia:transaction(F),
			
			slave:start(yesler, Symbol),
			spawn(quoteServer, start, [Symbol]),
			
			processSymbols(SymbolsSublist)
	end.

loop () ->
	receive
		Other ->
			erlang:display("symbolServer: Unknown request."),
			erlang:display(lists:concat(["symbolServer: ", Other])),
			loop()
	end.