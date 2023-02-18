import json
import logging
from typing import Optional, List

from helpers import JobTree, JobStatuses, JobFaultException, DUMP_FILE_NAME

logger = logging.getLogger()


class Scheduler:
    def __init__(self, pool_size: int = 10):
        assert pool_size > 0
        self.current_pool = []
        self.pool_size = pool_size
        self.success_pool = []
        self.fault_pool = []

    def __del__(self):
        """
        Если имеются незавершенные задачи - сохраняем список завершенных задач в файл
        """
        with open(DUMP_FILE_NAME, 'w') as file:
            file.write(self.__get_jobs_if_fault())

    def __get_jobs_if_fault(self) -> Optional[str]:
        """
        Если не все задачи выполнились - возвращаем список выполненных задач
        """
        has_unfinished_jobs = bool(JobTree.all_jobs())
        finished_jobs = [
            *self.success_pool,
            *self.fault_pool
        ]

        dump = json.dumps(finished_jobs, default=lambda x: x.name)

        return dump if has_unfinished_jobs else ''

    @staticmethod
    def _remove_jobs_from_dump(job_names: Optional[List[str]] = None) -> None:
        """
        Если в файле dump.json есть список выполненных задач - удаляем их из списка
        Для целей тестирования используется jobs_lst - чтобы не открывать файл в процессе теста
        """

        if not job_names:
            with open(DUMP_FILE_NAME, 'r') as file:
                data = file.read()

            if not data:
                return None

            job_names = json.loads(data)

        for job in JobTree.all_jobs():
            if job.name in job_names:
                JobTree.delete(job)

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

        # Перед стартом процесса выполнения задач проверяем наличие списка уже выполненных задач
        #   в файле dump.json
        #   Если они там есть - удаляем их из очереди
        #   Если нет - ничего не делаем. Значит предыдущий процесс выполнения запустился без ошибок

        self._remove_jobs_from_dump()

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
