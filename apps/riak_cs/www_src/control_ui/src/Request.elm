module Request exposing (listUsers, createUser, Status(..))

import Model exposing (Model)
import User exposing (User)
import Msg exposing (Msg(..))
import Http
import HttpBuilder
import Url
import Url.Builder as UrlBuilder
import Json.Encode as Encode
import Json.Decode as Decode exposing (list, string)
import Json.Decode.Pipeline exposing (required, optional, hardcoded)
import MD5
import Time
import Iso8601
import Bytes.Encode
import Base64
import HmacSha1
import HmacSha1.Key

type Status a = Loading | Loaded a | Failure

type alias UserFields =
    { name : String
    , email : String
    }


listUsers : Model -> Cmd Msg
listUsers m =
    m.config.csUrl ++ "/riak-cs/users"
        |> HttpBuilder.get
        |> HttpBuilder.withHeaders (makeHeaders "GET" m.config.csAdminSecret "" m.time)
        |> HttpBuilder.withExpect (Http.expectJson GotListUsers usersDecoder)
        |> HttpBuilder.request

createUser : Model -> String -> String -> Cmd Msg
createUser m name email  =
    let
        json = Encode.object [ ("name", Encode.string name)
                             , ("email", Encode.string email)
                             ]
    in
        m.config.csUrl ++ "/riak-cs/users"
            |> HttpBuilder.post
            |> HttpBuilder.withHeaders (makeHeaders "POST"
                                            m.config.csAdminSecret (json |> Encode.encode 0 |> MD5.hex)
                                            m.time)
            |> HttpBuilder.withJsonBody json
            |> HttpBuilder.withExpect (Http.expectWhatever UserCreated)
            |> HttpBuilder.request


usersDecoder =
    list userDecoder

userDecoder =
  Decode.succeed User
    |> required "name" string
    |> required "display_name" string
    |> required "email" string
    |> required "key_id" string
    |> required "secret_key" string
    |> required "id" string
    |> required "status" string
    |> required "policies" (list string)
    |> required "tags" (list string)
    |> required "buckets" (list string)

userEncoder u =
    Encode.object u


makeHeaders verb authSecret cmd5 time =
    let
        date = Iso8601.fromTime time
    in
        [ ("authorization", makeSignature verb authSecret cmd5 date)
        , ("content-type", "application/json")
        , ("content-md5", cmd5)
        , ("date", date)
        ]


makeSignature verb authSecret cmd5 date =
    let
        key = HmacSha1.Key.fromString authSecret
        str = verb ++ "\n" ++ cmd5 ++ "\napplication/json\n" ++ date ++ "\n" ++ "/riak-cs/users"
        sig =
            case Bytes.Encode.string (HmacSha1.fromString key str |> HmacSha1.toBase64)
                  |> Bytes.Encode.encode
                  |> Base64.fromBytes of
                Just s ->
                    s
                _ ->
                    ""
    in
        sig
