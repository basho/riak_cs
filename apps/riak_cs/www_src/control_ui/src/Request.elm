module Requests exposing (ListUsers, CreateUser)

import Http
import HttpBuilder
import Url.Builder as UrlBuilder
import Json.Encode as Encode
import Json.Decode as Decode
import MD5
import Time
import Iso8601
import Bytes.Encode
import Base64
import HmacSha1


type Msg = GotListUsers (Result Http.Error (List String))
type Status a = Loading | Loaded a | Failure


ListUsers : String -> String -> Cmd Msg
ListUsers baseUrl authSecret =
    Url.fromString baseUrl ["riak-cs", "users"]
        |> HttpBuilder.get
        |> HttpBuilder.withHeaders (makeHeaders "GET" authSecret "")
        |> HttpBuilder.withExpect (Http.expectJson GotListUsers usersDecoder)
        |> HttpBuilder.request

CreateUser : String -> String -> Cmd Msg
CreateUser baseUrl authSecret userFields =
    Url.fromString baseUrl ["riak-cs", "users"]
        |> HttpBuilder.post
        |> HttpBuilder.withHeaders (makeHeaders "POST" authSecret (content |> Encode |> MD5.hex))
        |> HttpBuilder.withJsonBody userFields
        |> HttpBuilder.withExpect (Http.expectWhatever UserCreated)
        |> HttpBuilder.request

usersDecoder =
    Decode.list Decode.string

userEncoder u =
    Encode.object u


makeHeaders verb authSecret cmd5 =
    let
        date = Iso8601.fromTime Time.now
    in
        [ ("authorization", makeSignature verb authSecret cmd5 date)
        , ("content-type", "application/json")
        , ("content-md5", cmd5)
        , ("date", date)
        ]


makeSignature verb authSecret cmd5 date =
    let
        str = verb ++ "\n" ++ cmd5 ++ "\napplication/json\n" ++ date ++ "\n" ++ "/riak-cs/users"
    in
        Bytes.Encode.string (HmacSha1.fromString (HmacSha1.Key.fromString authSecret) str)
            |> Bytes.Encode.encode
            |> Base64.fromBytes
