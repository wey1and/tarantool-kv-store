import requests
import json
import time

url = 'http://127.0.0.1:8080/kv/'
headers = {'Content-Type': 'application/json'}
key = 'test'
payload = {'key': key, 'value': 'ok'}
requests.delete(url + key, headers=headers)

def test_should_create_row():
    # when
    createResponse = requests.post(url, headers=headers, data=json.dumps(payload, indent=4))
    getResponse = requests.get(url + key, headers=headers)

    getResponseBody = getResponse.json()
    createResponseBody = createResponse.json()

    # then
    assert createResponse.status_code == 200
    assert getResponse.status_code == 200

    assert createResponseBody['message'] == 'Success!'
    assert getResponseBody == payload


def test_should_update_row():
    # given
    updatePayload = {'value': 'update'}

    requests.delete(url + key, headers=headers)
    createResponse = requests.post(url, headers=headers, data=json.dumps(payload, indent=4))

    # when
    updateResponse = requests.put(url + key, headers=headers, data=json.dumps(updatePayload, indent=4))
    getResponse = requests.get(url + key, headers=headers)

    getResponseBody = getResponse.json()
    updateResponseBody = updateResponse.json()

    # then
    assert updateResponse.status_code == 200
    assert updateResponseBody['message'] == 'Success!'
    assert getResponseBody['value'] == 'update'


def test_should_return_error_message_if_body_is_invalid():
    # given
    invalidPayload = {'invalid': 'body'}

    requests.delete(url + key, headers=headers)
    createResponse = requests.post(url, headers=headers, data=json.dumps(payload, indent=4))

    # when
    updateResponse = requests.put(url + key, headers=headers, data=json.dumps(invalidPayload, indent=4))
    updateResponseBody = updateResponse.json()

    # then
    assert updateResponse.status_code == 400
    assert updateResponseBody['message'] == 'Invalid JSON body'


def test_should_return_404_if_key_not_found():
    time.sleep(15)
    requests.delete(url + key, headers=headers)

    # when
    getResponse = requests.get(url + key, headers=headers)
    getResponseBody = getResponse.json()

    # then
    assert getResponse.status_code == 404
    assert getResponseBody['message'] == 'Key doesn\'t exist'


def test_should_return_error_message_if_request_limit_exceeded():
    time.sleep(15)
    # given
    key = 'test'
    payload = {'key': key, 'value': 'ok'}
    createResponse = requests.post(url, headers=headers, data=json.dumps(payload, indent=4))

    # when
    for i in range(11):
        getResponse = requests.get(url + key, headers=headers)
        body = getResponse.json()
        # then
        print(getResponse.status_code)
        if i >= 9:
            assert getResponse.status_code == 429
            assert body['message'] == 'Too many requests!'
        else:
            assert getResponse.status_code == 200

