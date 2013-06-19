#!/bin/sh

usage()
{
    echo "$0 [HOST PORT]"
}

if [ -z "$1" ]
then
    HOST="localhost"
else
      HOST=$1
fi

if [ -z "$2" ]
then
    PORT=${CS_HTTP_PORT:=8080}
else
    PORT=$2
fi

PARSECREDS="import json
import sys
user_input = sys.stdin.read()
user_json = json.loads(user_input)
sys.stdout.write(user_json['key_id'] + ' ' + user_json['key_secret'] + ' ' + user_json['id'])
sys.stdout.flush()"

NAME1=`uuidgen`
EMAIL1="$NAME1@s3-test.basho"
CREDS1=`curl -s --data "{\"email\":\"$EMAIL1\", \"name\":\"$NAME1\"}" -H 'Content-Type: application/json' "http://$HOST:$PORT/riak-cs/user" | python -c "$PARSECREDS"`
KEYID1=`echo $CREDS1 | awk '{print $1}'`
KEYSECRET1=`echo $CREDS1 | awk '{print $2}'`
CANONICALID1=`echo $CREDS1 | awk '{print $3}'`

NAME2=`uuidgen`
EMAIL2="$NAME2@s3-test.basho"
CREDS2=`curl -s --data "{\"email\":\"$EMAIL2\", \"name\":\"$NAME2\"}" -H 'Content-Type: application/json' "http://$HOST:$PORT/riak-cs/user" | python -c "$PARSECREDS"`
KEYID2=`echo $CREDS2 | awk '{print $1}'`
KEYSECRET2=`echo $CREDS2 | awk '{print $2}'`
CANONICALID2=`echo $CREDS2 | awk '{print $3}'`

CONFIG="[DEFAULT]
## this section is just used as default for all the \"s3 *\"
## sections, you can place these variables also directly there

## replace with e.g. \"localhost\" to run against local software
host = s3.amazonaws.com

## uncomment the port to use something other than 80
port = 80

proxy = $HOST
proxy_port = $PORT

## say \"no\" to disable TLS
is_secure = no

[fixtures]
## all the buckets created will start with this prefix;
## {random} will be filled with random characters to pad
## the prefix to 30 characters long, and avoid collisions
bucket prefix = cs-s3-tests-{random}-

[s3 main]
## the tests assume two accounts are defined, \"main\" and \"alt\".

## user_id is a 64-character hexstring
user_id = $CANONICALID1

## display name typically looks more like a unix login, \"jdoe\" etc
display_name = $NAME1

## replace these with your access keys
access_key = $KEYID1
secret_key = $KEYSECRET1

[s3 alt]
## another user account, used for ACL-related tests
user_id = $CANONICALID2
display_name = $NAME2
## the \"alt\" user needs to have email set, too
email = $EMAIL2
access_key = $KEYID2
secret_key = $KEYSECRET2"

echo "$CONFIG"
