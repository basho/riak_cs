require 'httparty'
require 'json'
require 'uuid'

def create_user(name)
  response = HTTParty.put("http://localhost:8080/riak-cs/user",
                          :body => {
                            :name => name,
                            :email => "#{name}@example.com"}.to_json,
                          :headers => {
                            "Content-Type" => "application/json"})
  json_body = JSON.parse(response.body)
  return json_body['key_id'], json_body['key_secret']
end

def s3_conf
  key_id, key_secret = create_user(UUID::generate)
  {
    access_key_id: key_id,
    secret_access_key: key_secret,
    proxy_uri: "http://localhost:8080",
    use_ssl: false,
    max_retries: 0
  }
end
