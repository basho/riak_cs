module View exposing (view)

import Html exposing (Html, text, div, node)
import Html.Attributes exposing (attribute, style, class, href, value, disabled, align, pattern, property, hidden)
import Html.Events exposing (onClick, onInput)
import Json.Encode
import Round
import Polymer.App as App
import Polymer.Paper as Paper exposing (button, input, item)
import Polymer.Attributes exposing (label)
import Model exposing (Model)
import Msg exposing (Msg(..), Action(..))


--import Routes exposing (Route(..))


view : Model -> Html Msg
view model =
    App.drawerLayout
        []
        [ drawer model
        , body model
        ]


drawer : Model -> Html Msg
drawer model =
    App.drawer
        []
        [ div
            [ class "drawer-contents" ]
            [ App.toolbar
                []
                [ div [] [ text "Menu" ] ]
            , Paper.menu
                [ attribute "selected" "x" ]
                [ Paper.item [] [ text "Test" ]
                ]
            ]
        ]


body : Model -> Html Msg
body model =
    App.headerLayout
        []
        [ App.header
            [ attribute "effects" "waterfall"
            , attribute "fixed" ""
            ]
            [ App.toolbar
                []
                [ Paper.iconButton
                    [ attribute "icon" "menu"
                    , attribute "drawer-toggle" ""
                    ]
                    []
                , div
                    [ class "title" ]
                    [ text "Riak CS Control" ]
                ]
            ]
        , Paper.card
            []
            [ makeControlButton model
            , makeStatsButton model

            -- static
            , makeStatusLine model
            ]
        , makeStats model
        ]

makeStatsButton : Model -> Html Msg
makeStatsButton model =
    Paper.button
        [ property "raised" (Json.Encode.bool (not model.state.displayStats))
        , onClick <| ToggleStats
        ]
        [ text "Stats" ]


makeRow : StatsItem -> Html msg
makeRow stat =
    Html.tr []
        [ Html.td [] [ stat.id |> toString |> text ]
        , Html.td [] [ stat.chunksSent |> toString |> text ]
        , Html.td [] [ stat.bytesSent |> toString |> text ]
        , Html.td [] [ stat.avgDeliveryTime |> Round.round 2 |> text ]
        , Html.td [] [ stat.timesReopened |> toString |> text ]
        ]

makeStatusLine : Model -> Html Msg
makeStatusLine model =
    let
        notifClass =
            case model.state.statusCode of
                0 ->
                    "status-line-ready"

                1 ->
                    "status-line-streaming"

                _ ->
                    "status-line-error"
    in
        Paper.item
            [ class notifClass ]
            [ text model.state.message ]
