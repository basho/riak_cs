-module(random_test).

-compile(export_all).

test1(X) ->
    timer:tc(?MODULE, crypto_test, [X]).

test2(X) ->
    timer:tc(?MODULE, random_test, [X]).

crypto_test(X) ->
    [integer_to_list(crypto:rand_uniform(0, 1000)) || _ <- lists:seq(1, X)].

random_test(X) ->
    [begin
         random:seed(os:timestamp()),
         integer_to_list(random:uniform(1000))
     end || _ <- lists:seq(1, X)].
