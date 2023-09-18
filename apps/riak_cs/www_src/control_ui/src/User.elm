module User exposing (User, Status(..))

type alias User =
    { name : String
    , display_name : String
    , email : String
    , keyId : String
    , secretKey : String
    , id : String
    , status : String
    , policies : List String
    , tags : List String
    , buckets : List String
    }

type Status
    = Enabled | Disabled
