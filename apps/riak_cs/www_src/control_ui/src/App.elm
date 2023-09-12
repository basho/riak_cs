module App exposing (..)

import Model exposing (Model)
import Msg exposing (Msg(..))
import UrlParser as Url

type alias Flags =
    { cs_control_port: String
    , cs_port: String
    , cs_host: String
    , cs_proto: String
    , cs_admin_key: String
    , cs_admin_secret: String
    , log_level: String
    , log_dir: String
    }


init : (Flags) -> (Model, Cmd Msg)
init flags =
    ( Model
          { config = {
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

readConfig 
