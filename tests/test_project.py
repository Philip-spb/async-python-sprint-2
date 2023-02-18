from helpers import JobTree


def test_remove_from_jobtree_registry(jobs, scheduler):
    last_job = jobs[-1:]
    job_names = [job.name for job in last_job]
    scheduler._remove_jobs_from_dump(job_names)
    assert True


def test_job_tree(jobs, scheduler):
    incoming_jobs = set(jobs)
    all_jobs_from_tree = JobTree.all_jobs()
    assert incoming_jobs == all_jobs_from_tree
