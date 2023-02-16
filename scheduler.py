import logging

from helpers import JobTree, JobStatuses, JobFaultException

logger = logging.getLogger()


class Scheduler:
    def __init__(self, pool_size: int = 10):
        assert pool_size > 0
        self.current_pool = []
        self.pool_size = pool_size
        self.success_pool = []
        self.fault_pool = []

    def schedule(self, task: 'Job'):
        parent_job = JobTree(task)
        if len(task.dependencies):
            for job in task.dependencies:
                descendant_job = JobTree.get_node(job)
                if not descendant_job:
                    descendant_job = JobTree(job)
                parent_job.set_relations(descendant_job)

    def _fill_pool(self) -> None:
        pool = [leaf.job for leaf in JobTree.get_leafs()]
        pool = sorted(pool, key=lambda x: x.start_at)

        new_jobs = set(pool) - set(self.current_pool)
        if not new_jobs:
            return

        for job in new_jobs:
            if len(self.current_pool) > self.pool_size - 1:
                return
            self.current_pool.append(job)

        return

    def __finish_job(self, job: 'Job'):
        logger.info(f'Delete job: {job}')
        if job.status == JobStatuses.SUCCESS:
            JobTree.delete(job)
            self.success_pool.append(job)
        else:
            job_node = JobTree.get_node(job)
            fault_job_nodes = job_node.get_descendants()
            fault_jobs = JobTree.del_nodes(fault_job_nodes)
            self.fault_pool.extend(fault_jobs)

    def run(self):

        self._fill_pool()
        while len(self.current_pool):
            job = self.current_pool.pop(0)
            try:
                next(job.run())
                self.current_pool.append(job)
            except StopIteration:
                self.__finish_job(job)
                self._fill_pool()
            except JobFaultException:
                if job.tries > 1:
                    job.tries -= 1
                    job.status = JobStatuses.NOT_STARTED
                    logger.warning(f'Restart job: {job}')
                    self.current_pool.append(job)
                else:
                    self.__finish_job(job)
                    self._fill_pool()

    # def restart(self):
    #     pass
    #
    # def stop(self):
    #     pass
