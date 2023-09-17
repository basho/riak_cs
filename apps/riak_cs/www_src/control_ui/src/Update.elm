module Update exposing (update)

import Model exposing (Model, Config, State)
import Msg exposing (Msg(..), Action(..))
import User exposing (GotListUsersResponse)
import Request exposing (Status(..), ListUsers, CreateUser)

update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        GotListUsers (Ok users) ->
            let
                oldState =
                    model.state
            in
                { model | state = { oldState | users = Loaded users, message = ""} } ! []

        GotListUsers (Err err) ->
            let
                oldState =
                    model.state
            in
                { model | state = {oldState | items = [], message = "Couldn't fetch users" } } ! Cmd.none

        UserCreated ->
            model ! ListUsers model.config.cs_url

        NoOp ->
            model ! []
