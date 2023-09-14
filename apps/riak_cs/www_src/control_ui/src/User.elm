module User exposing (User, Status(..))

type alias User =
    { name : String
    , display_name : String
    , email : String
    , key_id : String
    , secret_key : String
    , id : String
    , policies : List String
    , tags : List String
    , status : Status
    , buckets : List String
    }

type Status
    = Enabled | Disabled
