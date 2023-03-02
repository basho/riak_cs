-module(netutils).
-author('Juan Jose Comellas <juanjo@comellas.org>').

%% API
-export([get_local_ip_from_subnet/1, get_ip_from_subnet/2,
         cidr_network/1, cidr_netmask/1]).

%% @type ipv4() = {integer(), integer(), integer(), integer()}


%%--------------------------------------------------------------------
%% @spec get_local_ip_from_subnet({Network :: ipv4(), Netmask :: ipv4()}) ->
%%         {ok, ipv4()} | undefined | {error, Reason, Data}
%% @doc  Returns an IP address that is assigned to a local network interface and
%%       belongs to the specified subnet.
%%--------------------------------------------------------------------
get_local_ip_from_subnet({{_I1, _I2, _I3, _I4}, {_N1, _N2, _N3, _N4}} = Subnet) ->
    case inet:getif() of
        {ok, AddrList} when is_list(AddrList) ->
            get_ip_from_subnet(Subnet, [Ip || {Ip, _Broadcast, _Netmask} <- AddrList]);
        InvalidAddrList ->
            {error, invalid_network_interface, [InvalidAddrList]}
    end;
get_local_ip_from_subnet({{_I1, _I2, _I3, _I4} = Ip, Bits})
  when is_integer(Bits), Bits >= 0, Bits =< 32 ->
    get_local_ip_from_subnet({Ip, cidr_netmask(Bits)}).



%%--------------------------------------------------------------------
%% @spec get_ip_from_subnet({Network :: ipv4(), Netmask :: ipv4(), [ipv4()]}) ->
%%         {ok, ipv4()} | undefined | {error, Reason, Data}
%% @doc  Returns the first IP address in the list received as argument that
%%       belongs to the specified subnet.
%%--------------------------------------------------------------------
get_ip_from_subnet({{I1, I2, I3, I4}, {N1, N2, N3, N4} = Netmask}, AddrList) ->
    get_ip_from_normalized_subnet({{I1 band N1, I2 band N2, I3 band N3, I4 band N4},
                                   Netmask}, AddrList);
get_ip_from_subnet({{_I1, _I2, _I3, _I4} = Ip, Bits}, AddrList)
  when is_integer(Bits), Bits >= 0, Bits =< 32 ->
    get_ip_from_subnet({Ip, cidr_netmask(Bits)}, AddrList).


get_ip_from_normalized_subnet({{I1, I2, I3, I4}, {N1, N2, N3, N4}} = Subnet,
                              [{A1, A2, A3, A4} = Addr | Tail]) ->
    if ((A1 band N1) =:= I1 andalso
        (A2 band N2) =:= I2 andalso
        (A3 band N3) =:= I3 andalso
        (A4 band N4) =:= I4) ->
            {ok, Addr};
       true ->
            get_ip_from_normalized_subnet(Subnet, Tail)
    end;
get_ip_from_normalized_subnet(_Subnet, []) ->
    undefined.


%%--------------------------------------------------------------------
%% @spec cidr_network({Addr :: ipv4(), Bits :: integer()}) -> ipv4()
%% @doc  Return the subnet corresponding the the IP address and network bits
%%       received in CIDR format.
%%--------------------------------------------------------------------
cidr_network({{I1, I2, I3, I4}, Bits}) when is_integer(Bits) andalso Bits =< 32 ->
    ZeroBits = 8 - (Bits rem 8),
    Last = (16#ff bsr ZeroBits) bsl ZeroBits,

    case (Bits div 8) of
        0 ->
            {(I1 band Last), 0, 0, 0};
        1 ->
            {I1, (I2 band Last), 0, 0};
        2 ->
            {I1, I2, (I3 band Last), 0};
        3 ->
            {I1, I2, I3, (I4 band Last)};
        4 ->
            {I1, I2, I3, I4}
    end.


%%--------------------------------------------------------------------
%% @spec cidr_netmask(Bits :: integer()) -> ipv4()
%% @doc  Return the netmask corresponding to the network bits received in CIDR
%%       format.
%%--------------------------------------------------------------------
cidr_netmask(Bits) when is_integer(Bits) andalso Bits =< 32 ->
    ZeroBits = 8 - (Bits rem 8),
    Last = (16#ff bsr ZeroBits) bsl ZeroBits,

    case (Bits div 8) of
        0 ->
            {(255 band Last), 0, 0, 0};
        1 ->
            {255, (255 band Last), 0, 0};
        2 ->
            {255, 255, (255 band Last), 0};
        3 ->
            {255, 255, 255, (255 band Last)};
        4 ->
            {255, 255, 255, 255}
    end.
