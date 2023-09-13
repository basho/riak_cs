module App exposing (..)

import Model exposing (Model)
import Msg exposing (Msg(..))

type alias Flags =
    { cs_port: String
    , cs_host: String
    , cs_proto: String
    , cs_admin_key: String
    , cs_admin_secret: String
    }


init : (Flags) -> (Model, Cmd Msg)
init flags =
    ( Model
          { config = flags
          , state =
                { users = []
                , status = Ok
                , message = ""
                }
          }
    , Http.get
          { url = url
          , expect = Http.expectString ListUsersResponse
          }
  )
