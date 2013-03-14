require 'httparty'
require 'json'
require 'uuid'
require 'tempfile'

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
    http_read_timeout: 2000,
    max_retries: 0
  }
end

def new_mb_temp_file(size)
    temp = Tempfile.new 'riakcs-test'
    (size*1024*1024).times {|i| temp.write 0}
    temp
end
