module Main exposing (..)

import App exposing (init, subscriptions)
import View exposing (view)
import Update exposing (update)
import Msg exposing (Msg)
import Model exposing (Model)
import Browser

main : Program Flags Model Msg
main =
  Browser.element
    { init = init
    , update = update
    , subscriptions = subscriptions
    , view = view
    }
