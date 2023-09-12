module Model
    exposing
        ( Model
        , Config
        , State
        )

import Routes exposing (Route(..))


type alias Model =
    { config : Config
    , state : State
    }


type alias Config =
    { rcsHost : String
    , admin_key_id : String
    , admin_secret : String
    }

type alias State =
    { users : List User
    , status : Status
    , message : String
    }

type Status
    = Ok
    | Error String.

type alias User =
    { name : String
    , display_name : String
    , email : String
    , key_id : String
    , secret_key : String
    , id : String
    , policies : List String
    , tags : List String
    , status : UserStatus
    , buckets : List String
    }

type UserStatus
    = Enabled | Disabled.
