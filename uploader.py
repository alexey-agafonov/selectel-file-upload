#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import logging
import time
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional

import requests
from dotenv import load_dotenv


class Uploader:
    executor = ThreadPoolExecutor(os.cpu_count() + 4)

    API_URL: str = 'https://api.selcdn.ru'
    AUTH_TOKENS_URL: str = f'{API_URL}/v3/auth/tokens'
    access_token: str = ''
    access_token_expire_date: datetime = None

    def __init__(self, root_dir: str, selectel_id: str, selectel_password: str) -> None:
        self.root_dir = root_dir
        self.selectel_id = selectel_id
        self.selectel_password = selectel_password
        self.logger = logging.getLogger('uploader')
        logging.basicConfig(level='INFO', format='%(message)s',
                            datefmt="[%d.%m.%Y %H:%M:%S]")

    def __get_access_token(self) -> Optional[str]:
        now: datetime = datetime.utcnow()
        if self.access_token != '' and self.access_token_expire_date and self.access_token_expire_date > now:
            return self.access_token

        data = {
            "auth": {
                "identity": {
                    "methods": [
                        "password"
                    ],
                    "password": {
                        "user": {
                            "id": self.selectel_id,
                            "password": self.selectel_password
                        }
                    }
                }
            }
        }

        try:
            response = requests.post(self.AUTH_TOKENS_URL, data=json.dumps(data))
        except requests.exceptions.RequestException:
            return None

        if response.status_code == 201:
            self.access_token = response.headers.get('X-Subject-Token')
            tmp = json.loads(response.content)
            self.access_token_expire_date = datetime.strptime(tmp['token']['expires_at'], '%Y-%m-%dT%H:%M:%S.%fZ')
            return self.access_token
        else:
            return None

    def __send_file_to_container(self, location: str, part: str):
        try:
            container: str = os.path.basename(os.path.dirname(location))
            filename: str = os.path.basename(os.path.normpath(location))
            full_path: str = os.path.join(location, part)
            data = open(full_path, 'rb').read()
            access_token = self.__get_access_token()

            headers = {'X-Auth-Token': access_token,
                       "Content-Type": "application/binary"}

            resp = requests.put(f'{self.API_URL}/v1/SEL_{self.selectel_id}/{container}/{filename}/{part}',
                                data=data, headers=headers)
            self.logger.info(f'File {filename} has successfully pushed to the cloud storage.')

            if resp.status_code == 201:
                if os.path.exists(full_path):
                    os.remove(full_path)
                    self.logger.info(f'File {filename} has successfully deleted.')
        except Exception as ex:
            self.logger.info(ex)

    def watch(self):
        for subdir, dirs, files in os.walk(self.root_dir):
            for file in files:
                self.executor.submit(self.__send_file_to_container, subdir, file)


if __name__ == "__main__":
    load_dotenv()
    root_dir = os.getenv('ROOT_DIR')
    selectel_id = os.getenv('SELECTEL_USER_ID')
    selectel_password = os.getenv('SELECTEL_PASSWORD')
    uploader = Uploader(root_dir=root_dir, selectel_id=selectel_id, selectel_password=selectel_password)

    while True:
        uploader.watch()
        time.sleep(60)
