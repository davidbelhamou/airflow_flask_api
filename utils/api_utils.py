import time
from datetime import datetime
import os.path
from typing import Any
import requests
import json
from flask import make_response, jsonify, Response
from requests.auth import HTTPBasicAuth
from requests.adapters import Retry, HTTPAdapter
from croniter import croniter


def http_request(authentication: HTTPBasicAuth, url: str, request_type: str = 'GET', data: dict = None):
    """HTTP requests

    :param data: (dict) of the data to pass in the request can be None
    :param url: (str) url for the request
    :param request_type:  (str) GET/POST/DELETE/PUT/PATCH
    :type authentication: (object) authentication to airflow api
    """
    try:
        if request_type == 'DELETE':
            response = requests.delete(url, auth=authentication)
        elif request_type == 'POST':
            response = requests.post(url, auth=authentication)
        elif request_type == 'PUT':
            response = requests.put(url, auth=authentication, data=data)
        elif request_type == 'PATCH':
            response = requests.patch(url, auth=authentication, data=data)
        else:
            response = requests.get(url, auth=authentication)
    except Exception as ex:
        print(f'http request of type {request_type} with url: {url} failed, error: {ex}')
        raise Exception('request failed')
    return response.status_code, json.loads(response.text)


def find_dag_by_id(dag_id: str, airflow_api: str, authentication: HTTPBasicAuth) -> tuple[int, Any]:
    try:
        response = requests.get(airflow_api + f'/{dag_id}', auth=authentication)
        response_status_code, response_content = http_request(authentication, airflow_api + f'/{dag_id}', 'GET')
        return response_status_code, response_content
    except Exception as ex:
        print(f'Request if finding dag_id: {dag_id} failed , due to error: {ex}')
        raise Exception('request failed')


def delete_dag_by_id(response_content: dict, owner: str, authentication: HTTPBasicAuth, dag_id: str,
                     airflow_api: str, is_delete_permanently: str = False) -> Response:
    if 'owners' in response_content and owner in response_content['owners']:
        response_status_code, response_content = http_request(authentication, airflow_api + f'/{dag_id}', 'DELETE')
        if response_status_code == 204:
            if is_delete_permanently:
                if os.path.isfile(f'./{dag_id}.py'):
                    os.remove('./{dag_id}.py')
            return make_response(jsonify({'message': f'dag:{dag_id} have been deleted'}), 200)
    else:
        return make_response(jsonify({'message': f'{owner} is not the owner of : {dag_id}'}), 403)


def edit_cron_dag_by_id(dag_id: str, authentication: HTTPBasicAuth, new_cron_expression: str, airflow_api: str):
    try:
        response_status_code, response_content = http_request(authentication, airflow_api + '/' + f'{dag_id}', 'GET')
        if response_status_code == 200:
            response_code, response_content = http_request(authentication, airflow_api + f'/{dag_id}', 'PATCH',
                                                           {"is_paused": True})
            old_cron = response_content['schedule_interval'].get('value')

    except Exception as ex:
        print(ex)


def validate_cron(cron_expression: str):
    time = datetime.now()
    base_time = datetime(time.year, time.month, time.day, time.hour, time.minute)
    datetime_iter = croniter(cron_expression, base_time)
    next_exec = datetime_iter.get_next(datetime)
    if (next_exec - base_time).total_seconds() / 60 <= 60:
        return False
    return True


def activate_dag(api, auth, dag_id):
    is_paused = {"is_paused": True}
    wait, tries, res = 30, 5, 500
    while tries > 0:
        res = requests.patch(api + f'/{dag_id}', json=is_paused, auth=auth)
        if res.status_code == 200:
            break
        time.sleep(wait)
        tries -= 1
    return res.status_code