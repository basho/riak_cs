module App exposing (..)

import Model exposing (Model)
import Msg exposing (Msg(..))

type alias Flags =
    { cs_url : String
    -- these should be collected from user; passing these as flags pending development
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
