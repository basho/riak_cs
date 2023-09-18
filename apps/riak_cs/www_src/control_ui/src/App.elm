module App exposing (init, subscriptions, Flags)

import Model exposing (Model, Config, State)
import Msg exposing (Msg(..))
import Request exposing (listUsers, createUser)
import Time

type alias Flags =
    { csUrl : String
    -- these should be collected from user; passing these as flags pending development
    , csAdminKey: String
    , csAdminSecret: String
    }



init : (Flags) -> (Model, Cmd Msg)
init flags =
    let
        config = Config flags.csUrl flags.csAdminKey flags.csAdminSecret
        state = State [] (Ok ())
        model = Model config state (Time.millisToPosix 0)
    in
        ( model
        , listUsers model
        )


subscriptions : Model -> Sub Msg
subscriptions _ =
    Time.every 1000 Tick
