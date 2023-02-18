import os
import logging
from typing import Optional, List
from datetime import datetime, timedelta
from threading import Thread
import requests

from helpers import (JobStatuses, JobFaultException, FileJobStatuses, DirectoryJobStatuses,
                     WORK_DIR_NAME)

logger = logging.getLogger()
CWD = os.getcwd()


class Job:
    def __init__(
            self,
            name: Optional[str] = None,
            start_at: str = '',
            max_working_time: int = -1,
            tries: int = 0,
            dependencies: Optional[List['Job']] = None
    ):

        if not isinstance(dependencies, list):
            dependencies = []

        start_at_datetime = datetime.now()
        if start_at:
            try:
                start_at_datetime = datetime.strptime(start_at, '%Y-%m-%d %H:%M:%S')
                if start_at_datetime < datetime.now():
                    start_at_datetime = datetime.now()
            except ValueError as e:
                logger.error(e)

        self.name = name
        # self.uuid = uuid.uuid4()
        self.tread = None
        self.status = JobStatuses.NOT_STARTED
        self.tries = tries  # Сколько попыток дается задаче
        self.current_tries = 0  # Текущее количество использованных попыток
        self.actual_start_time = False  # Точное время в которое задача начала выполнение
        self.start_at = start_at_datetime  # В какое время необходимо стартовать задачу
        self.max_working_time = max_working_time  # Максимальное время отведенное задаче

        # Список задач которые должны быть завершены перед стартом текущей задачи
        self.dependencies = dependencies

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return self.__str__()

    def _get_data(self):
        ...

    def __process(self):
        if not self.tread:
            self.tread = Thread(target=self._get_data, daemon=True)
            self.tread.start()

        if self.status == JobStatuses.FAULT:
            self.tread = None
            raise JobFaultException
        elif self.status == JobStatuses.SUCCESS:
            self.tread = None
            return

        yield

    def run(self):
        now = datetime.now()
        while self.start_at >= now:
            yield

        if not self.actual_start_time:
            self.actual_start_time = now

        if (
                self.max_working_time >= 0
                and self.actual_start_time + timedelta(seconds=self.max_working_time) < now
        ):
            logger.warning(f'{self.name}: time ({self.max_working_time} sec.) is over')
            self.status = JobStatuses.FAULT
            return

        if self.status == JobStatuses.NOT_STARTED:
            logger.info(f'Start {self.name}')
            self.status = JobStatuses.WORK

        yield from self.__process()


class DirectoryJob(Job):
    """
    Работа с файловой системой: создание, удаление, изменение директорий и файлов;
    """
    def __init__(self,
                 job_type: int = DirectoryJobStatuses.CREATE,
                 extra_dir: str = '',
                 dir_name: str = 'test',
                 new_dir_name: Optional[str] = '',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_type = job_type
        self.extra_dir = extra_dir
        self.dir_name = dir_name
        self.new_dir_name = new_dir_name

    def _get_data(self) -> None:
        try:
            path = f'{CWD}/{WORK_DIR_NAME}'
            if self.extra_dir:
                path = f'{path}/{self.extra_dir}'

            if self.job_type == DirectoryJobStatuses.CREATE:
                os.makedirs(f'{path}/{self.dir_name}', exist_ok=True)
                self.status = JobStatuses.SUCCESS

            elif self.job_type == DirectoryJobStatuses.RENAME:
                if not self.new_dir_name:
                    self.status = JobStatuses.FAULT
                    return
                os.renames(f'{path}/{self.dir_name}', f'{path}/{self.new_dir_name}')
                self.status = JobStatuses.SUCCESS

            elif self.job_type == DirectoryJobStatuses.DELETE_DIR:
                os.rmdir(f'{path}/{self.dir_name}')
                self.status = JobStatuses.SUCCESS

            elif self.job_type == DirectoryJobStatuses.DELETE_FILE:
                os.remove(f'{path}/{self.dir_name}')
                self.status = JobStatuses.SUCCESS

        except FileExistsError:
            self.status = JobStatuses.FAULT
            return

        except OSError as e:
            logger.error(f'{self.name}: {e}')
            self.status = JobStatuses.FAULT
            return


class FileJob(Job):
    """
    Работа с файлами: создание, чтение, запись;
    """
    def __init__(self,
                 job_type: int = FileJobStatuses.WRITE,
                 file_name: str = 'test.txt',
                 file_dir: str = '',
                 file_text: str = 'Hello, World!',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_type = job_type
        self.file_name = file_name
        self.file_dir = file_dir
        self.file_text = file_text

    def __create_file(self) -> None:
        path = f'{CWD}/{WORK_DIR_NAME}'
        if self.file_dir:
            path = f'{path}/{self.file_dir}'

        with open(f'{path}/{self.file_name}', mode='w') as f:
            f.write(self.file_text)
        self.status = JobStatuses.SUCCESS

    def __read_file(self) -> None:
        path = f'{CWD}/{WORK_DIR_NAME}'
        if self.file_dir:
            path = f'{path}/{self.file_dir}'

        with open(f'{path}/{self.file_name}', mode='r') as f:
            f.read()
        self.status = JobStatuses.SUCCESS

    def _get_data(self) -> None:
        try:
            if self.job_type == FileJobStatuses.WRITE:
                self.__create_file()
            else:
                self.__read_file()
        except FileNotFoundError:
            self.status = JobStatuses.FAULT
            return


class GetFromURLJob(Job):
    """
    Работа с сетью: обработка ссылок (GET-запросы) и анализ полученного результата;
    """

    def __init__(self, url: str, *args, **kwargs):
        self.url = url
        super().__init__(*args, **kwargs)

    def _get_data(self) -> None:
        try:
            result = requests.get(self.url)
        except requests.exceptions.ConnectionError:
            self.status = JobStatuses.FAULT
            return

        if result.status_code == 200:
            self.status = JobStatuses.SUCCESS
        else:
            self.status = JobStatuses.FAULT
