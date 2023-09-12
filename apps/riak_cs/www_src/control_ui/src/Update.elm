module Update exposing (update)

import UrlParser as Url
import Model exposing (Model, Config, State)
import Msg exposing (Msg(..), Action(..))
import Routes exposing (parseRoute)
import Json.Decode exposing (int, string, float, Decoder, nullable)
import Json.Decode.Pipeline exposing (decode, required, optional)
import Http

update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        GotListUsersResult users ->
            let
                oldState =
                    model.state
            in
                { model | state = { oldState | users = users } } ! []

        NoOp ->
            model ! []
