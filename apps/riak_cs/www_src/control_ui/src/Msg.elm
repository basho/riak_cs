module Msg
    exposing
        ( Msg(..)
        , Action(..)
        )

import User exposing (User)

type Msg
    = ListUsersResponse (List User)
      -- user input:
    | GotCreateUserResult User
      -- other
    | NoOp


type Action
    = ListUsers
    | CreateUser
    | EnableUser
    | DisableUser
    | RegenerateCreds
