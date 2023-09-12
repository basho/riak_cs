module Routes exposing (Route(..), parseRoute, toString)

import UrlParser as Url exposing ((</>), (<?>), s, int, stringParam, top)


type Route
    = Home


parseRoute : Url.Parser (Route -> a) a
parseRoute =
    Url.oneOf
        [ Url.map Home top
        ]


toString : Route -> String
toString route =
    case route of
        Home ->
            ""
