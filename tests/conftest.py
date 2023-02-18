from typing import List

import pytest

from helpers import JobTree
from job import Job
from scheduler import Scheduler


@pytest.fixture
def jobs() -> List[Job]:
    all_jobs = JobTree.all_jobs()

    if not all_jobs:
        job1 = Job(name='job1')
        job2 = Job(name='job2', dependencies=[job1, ])
        job3 = Job(name='job3', dependencies=[job2, ])
        job4 = Job(name='job4', dependencies=[job3, ])
        job5 = Job(name='job5', dependencies=[job4, job3])
        all_jobs = (job1, job2, job3, job4, job5)

    sorted_jobs = sorted(all_jobs, key=lambda x: x.name)

    return sorted_jobs


@pytest.fixture
def scheduler(jobs) -> Scheduler:
    scheduler = Scheduler()

    # Инициализируем JobTree только если раньше еще этого не делали
    if not JobTree.all_jobs():
        for job in jobs:
            scheduler.schedule(job)

    return scheduler
