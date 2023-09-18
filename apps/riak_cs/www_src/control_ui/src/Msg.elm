module Msg exposing (Msg(..), Action(..))

import User exposing (User)
import Http
import Time

type Msg
    = GotListUsers (Result Http.Error (List User))
    | UserCreated (Result Http.Error ())
    | Tick Time.Posix
    | NoOp


type Action
    = ListUsers
    | CreateUser
    | EnableUser
    | DisableUser
    | RegenerateCreds
