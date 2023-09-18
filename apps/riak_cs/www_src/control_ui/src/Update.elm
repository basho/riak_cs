module Update exposing (update)

import Model exposing (Model, Config, State)
import Msg exposing (Msg(..), Action(..))
import Request exposing (listUsers, Status(..))

update : Msg -> Model -> (Model, Cmd Msg)
update msg model =
    case msg of
        GotListUsers (Ok users) ->
            let
                oldState =
                    model.state
            in
                ( { model | state = { oldState | users = users, status = Ok ()} }
                , Cmd.none
                )

        GotListUsers (Err err) ->
            let
                oldState =
                    model.state
            in
                ( { model | state = {oldState | users = [], status = Err "Couldn't fetch users" } }
                , Cmd.none
                )

        UserCreated (Ok ()) ->
            ( model
            , listUsers model
            )

        UserCreated (Err err) ->
            let
                oldState =
                    model.state
            in
                ( { model | state = {oldState | status = Err "Failed to create user"} }
                , Cmd.none
                )

        Tick newTime ->
            ({ model | time = newTime }
            , Cmd.none
            )

        NoOp ->
            (model, Cmd.none)
