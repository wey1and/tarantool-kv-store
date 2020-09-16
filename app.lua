#!/usr/bin/env tarantool

local server = require('http.server').new('127.0.0.1', 8080)
local router = require('http.router').new()
local log = require('log')
local json = require('json')

box.cfg{
}

box.once('schema',
	function()
		box.schema.create_space('kv_store',
			{
				format = {
					{ name = 'key';   type = 'string' },
					{ name = 'value'; type = '*' },
				};
				if_not_exists = true;
			}
		)
		box.space.kv_store:create_index('primary',
			{ type = 'hash'; parts = {'key'}; if_not_exists = true; }
		)
	end
)

local HTTP_CODE = {
  SUCCESS = 200,
  NOT_FOUND = 404,
  CONFLICT = 409,
  BAD_REQUEST = 400,
  TOO_MANY_REQUESTS = 429
}

local THRESHOLD_SECONDS = 1;
local THRESHOLD_REQUESTS = 1;
local requestsCounter = 0;
local lastTimeApiCall = 0;

local function checkRps()
  currentTime = os.time()
  delta = currentTime - lastTimeApiCall
  requestsCounter = requestsCounter + 1

  if delta < THRESHOLD_SECONDS and requestsCounter > THRESHOLD_REQUESTS then
    requestsCounter = 0
    return false
  end
  lastTimeApiCall = currentTime
  return true
end

local function get_response(req, code, message)
  local response = req:render{ json = { message = message } }
  response.status = code
  return response
end


local function get_value(req)
  if checkRps() then
  local key = req:stash('key')
  local row = box.space.kv_store:select{ key }
  if row[1] == nil then
      return get_response(req, HTTP_CODE.NOT_FOUND, "Key doesn't exist")
  end
  local resp = req:render{json = {key = row[1][1], value = row[1][2]}}
  resp.status = HTTP_CODE.SUCCESS
  return resp
  end
  return get_response(req, HTTP_CODE.TOO_MANY_REQUESTS, "Too many requests!")
end

local function create_value(req)
  if checkRps() then
	local body = req:json()
	local key = body['key']
	local value = body['value']
	local row = box.space.kv_store:select{ key }
    if row[1] ~= nil then
        return get_response(req, HTTP_CODE.CONFLICT, "Key already exists")
    end

	box.space.kv_store:insert{ key, value }

  end
  return get_response(req, HTTP_CODE.TOO_MANY_REQUESTS, "Too many requests!")
end

--todo fix errors
local function update_value(req)
  if checkRps() then
  local key = req:stash('key')

  local status, value = pcall(function()
    local body = req:json()
    return body['value']
  end)
  local row = box.space.kv_store:select{ key }
  if row[1] == nil then
      return get_response(req, HTTP_CODE.NOT_FOUND, "Key doesn't exist")
  end
  local rowKey = row[1][1]

  local row = box.space.kv_store:update({rowKey}, {{'=', 2, value}})

  if not status then
    return get_response(req, HTTP_CODE.BAD_REQUEST, "Invalid JSON body")
  end

  return get_response(req, HTTP_CODE.SUCCESS, "Success!")
  end
  return get_response(req, HTTP_CODE.TOO_MANY_REQUESTS, "Too many requests!")
end

local function delete_value(req)
  if checkRps() then
  local key = req:stash('key')

  local row = box.space.kv_store:select{ key }
  if row[1] == nil then
     return get_response(req, HTTP_CODE.NOT_FOUND, "Key doesn't exist")
  end
  local rowKey = row[1][1]

  box.space.kv_store:delete({rowKey})

  return get_response(req, HTTP_CODE.SUCCESS, "Success!")
  end
  return get_response(req, HTTP_CODE.TOO_MANY_REQUESTS, "Too many requests!")
end






router:route({ path = '/kv', method = 'POST' }, create_value)
router:route({ path = '/kv/:key', method = 'GET' }, get_value)
router:route({ path = '/kv/:key', method = 'PUT' }, update_value)
router:route({ path = '/kv/:key', method = 'DELETE' }, delete_value)

server:set_router(router)
server:start()
