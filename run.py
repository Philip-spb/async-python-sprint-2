from helpers import FileJobStatuses, DirectoryJobStatuses
from job import DirectoryJob, FileJob, GetFromURLJob
from scheduler import Scheduler

if __name__ == '__main__':
    job0 = GetFromURLJob(
        name='job0',
        tries=5,
        url='https://mobimg.b-cdn.net/v3/fetch/8f/8f84ade8f5c56c6292d642a7b97c6f93.jpeg'
    )

    job1 = DirectoryJob(
        name='job1',
        job_type=DirectoryJobStatuses.DELETE,
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
        job_type=FileJobStatuses.READ,
        tries=7,
        start_at='2023-02-14 10:07:30'
    )

    job4 = GetFromURLJob(
        name='job4',
        tries=5,
        url='https://ya.ru'
        # url='https://mobimg.b-cdn.net/v3/fetch/8f/8f84ade8f5c56c6292d642a7b97c6f93.jpeg'
    )

    job5 = DirectoryJob(
        name='job5',
        job_type=DirectoryJobStatuses.DELETE,
        dir_name='test.txt',
        extra_dir='HELLO',
        # tries=5,
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

    job8 = FileJob(
        name='job8',
        file_dir='HELLO/',
        dependencies=[job4, ]
    )


    # job8 = FileJob(name='job8', dependencies=[job4, ])
    # job9 = FileJob(name='job9', dependencies=[job4, ])
    # job10 = FileJob(name='job10', dependencies=[job4, ])
    # job11 = FileJob(name='job11', dependencies=[job4, ])
    # job12 = FileJob(name='job12', dependencies=[job4, ])

    scheduler = Scheduler(pool_size=5)
    scheduler.schedule(job0)
    scheduler.schedule(job1)
    scheduler.schedule(job2)
    scheduler.schedule(job3)
    scheduler.schedule(job4)
    scheduler.schedule(job5)
    scheduler.schedule(job6)
    scheduler.schedule(job7)
    # scheduler.schedule(job8)

    # scheduler.schedule(job9)
    # scheduler.schedule(job10)
    # scheduler.schedule(job11)
    # scheduler.schedule(job12)

    # print(JobTree.get_leafs())

    scheduler.run()
