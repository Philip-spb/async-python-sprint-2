import logging

from helpers import FileJobStatuses, DirectoryJobStatuses
from job import DirectoryJob, FileJob, GetFromURLJob
from scheduler import Scheduler

logger = logging.getLogger()

if __name__ == '__main__':
    job0 = GetFromURLJob(
        name='job0',
        tries=5,
        url='https://mobimg.b-cdn.net/v3/fetch/8f/8f84ade8f5c56c6292d642a7b97c6f93.jpeg',
        max_working_time=2
    )

    job1 = DirectoryJob(
        name='job1',
        job_type=DirectoryJobStatuses.DELETE_DIR,
        dir_name='HELLO',
    )

    job2 = DirectoryJob(
        name='job2',
        job_type=DirectoryJobStatuses.CREATE,
        dir_name='test',
        tries=5,
        start_at='2023-02-14 8:07:30'
    )

    job3 = FileJob(
        name='job3',
        job_type=FileJobStatuses.WRITE,
        file_name='test.txt',
        file_dir='test',
        tries=7,
        start_at='2023-02-14 10:07:30',
        dependencies=[job2, ]
    )

    job4 = GetFromURLJob(
        name='job4',
        tries=5,
        url='https://ya.ru',
        # url='https://mobimg.b-cdn.net/v3/fetch/8f/8f84ade8f5c56c6292d642a7b97c6f93.jpeg',
        dependencies=[job3, ]
    )

    job5 = DirectoryJob(
        name='job5',
        job_type=DirectoryJobStatuses.DELETE_FILE,
        dir_name='test.txt',
        extra_dir='HELLO',
        # tries=5,
        dependencies=[job2, ]
    )

    job6 = DirectoryJob(
        name='job6',
        job_type=DirectoryJobStatuses.RENAME,
        dir_name='test',
        new_dir_name='HELLO',
        dependencies=[job2, ]
    )

    job7 = FileJob(
        name='job7',
        job_type=FileJobStatuses.WRITE,
        file_name='test.txt',
        file_dir='HELLO',
        dependencies=[job6, ]
    )

    scheduler = Scheduler(pool_size=5)

    scheduler.schedule(job0)
    scheduler.schedule(job1)
    scheduler.schedule(job2)
    scheduler.schedule(job3)
    scheduler.schedule(job4)
    scheduler.schedule(job5)
    scheduler.schedule(job6)
    scheduler.schedule(job7)

    scheduler.run()

    logger.info(f'{scheduler.success_pool=}')
    logger.info(f'{scheduler.fault_pool=}')
