module Model exposing (Model, Config, State)

import User exposing (User)
import Time


type alias Model =
    { config : Config
    , state : State
    , time : Time.Posix
    }


type alias Config =
    { csUrl: String
    , csAdminKey: String
    , csAdminSecret: String
    }

type alias State =
    { users : List User
    , status : Result String ()
    }
