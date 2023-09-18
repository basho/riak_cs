module Main exposing (..)

import App exposing (init, subscriptions, Flags)
import View exposing (view)
import Update exposing (update)
import Model exposing (Model)
import Msg exposing (Msg)
import Browser

main : Program Flags Model Msg
main =
  Browser.element
    { init = init
    , update = update
    , subscriptions = subscriptions
    , view = view
    }
