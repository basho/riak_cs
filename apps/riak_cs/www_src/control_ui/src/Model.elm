module Model
    exposing
        ( Model
        , Config
        , State
        )

import User exposing (User)


type alias Model =
    { config : Config
    , state : State
    }


type alias Config =
    { cs_url: String
    , cs_admin_key: String
    , cs_admin_secret: String
    }

type alias State =
    { users : List User
    , status : Status
    , message : String
    }

type Status
    = Ok
    | Error String
