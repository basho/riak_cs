module View exposing (view)

import Html exposing (Html, text, div, node)
import Html.Attributes exposing (attribute, style, class, href, value, disabled, align, pattern, property, hidden)
import Html.Events exposing (onClick, onInput)
import Material.TopAppBar as TopAppBar
import Material.Button as Button
import Material.Card as Card
import Material.IconButton as IconButton

import Model exposing (Model)
import Msg exposing (Msg(..), Action(..))
import User exposing (Status)



view : Model -> Html Msg
view model =
    TopAppBar.shortCollapsed TopAppBar.config
        [ TopAppBar.row []
            [ TopAppBar.section [ TopAppBar.alignStart ]
                [ IconButton.iconButton
                    (IconButton.config
                        |> IconButton.setAttributes
                            [ TopAppBar.navigationIcon ]
                    )
                    (IconButton.icon "menu")
                , Html.span [ TopAppBar.title ]
                    (makeUsers model)
                ]
            ]
        ]

makeUsers model =
    List.map makeUser model.state.users

makeUser user =
    Card.card Card.config
        { blocks =
            ( Card.block <|
                Html.div []
                    [ Html.h2 [] [ text user.name ]
                    , Html.h3 [] [ text user.email ]
                    ]
            , [ Card.block <|
                    Html.div []
                        [ Html.p [] [ text user.id ]
                        , Html.p [] [ text user.keyId]
                        ]
              ]
            )
        , actions =
            Just <|
                Card.actions
                    { buttons =
                        [ Card.button Button.config user.status ]
                    , icons =
                        [ Card.icon IconButton.config
                            (IconButton.icon "favorite")
                        ]
                    }
        }
