import logging
from enum import IntEnum
from typing import Set, Optional

logging.basicConfig(
    level='INFO',
    # filename='application-log.log',
    # filemode='w',
    format='%(asctime)s %(name)-30s %(levelname)-8s %(message)s'
)

WORK_DIR_NAME = 'work-dir'


class JobFaultException(Exception):
    ...


class NodeDeleteException(Exception):
    ...


class JobStatuses(IntEnum):
    NOT_STARTED = 0  # Ждет старта
    WORK = 1  # В процессе работы
    SUCCESS = 2  # Завершено успешно
    FAULT = 3  # Завершено с ошибкой


class FileJobStatuses(IntEnum):
    READ = 0  # Чтение
    WRITE = 1  # Запись


class DirectoryJobStatuses(IntEnum):
    CREATE = 0  # Создание
    DELETE = 1  # Удаление
    RENAME = 2  # Переименование


class JobTree:
    __registry = []

    def __init__(self, job: 'Job'):
        assert job not in JobTree.all_jobs(), 'Этот элемент уже добавлен'
        self.parent = []
        self.job = job
        self.__registry.append(self)

    def __str__(self) -> str:
        return f'Узел для задачи {self.job}'

    def set_descendant(self, child: 'JobTree') -> None:
        child.parent.append(self)

    def get_ancestors(self) -> Set['Job']:
        # TODO Реализовать метод возвращающий множество всех предков
        #   Потребуется для того чтобы "прокинуть" ошибку всем предкам
        ancestors = []

        return set(ancestors)

    @classmethod
    def delete(cls, job: 'Job'):
        try:
            node = cls.get_node(job)
            cls.__registry.remove(node)
        except ValueError:
            raise NodeDeleteException

    @classmethod
    def _get_parents(cls) -> set:
        parents = []
        for node in cls.__registry:
            parents.extend(node.parent)
        return set(parents)

    @classmethod
    def all_nodes(cls) -> list:
        return cls.__registry

    @classmethod
    def all_jobs(cls) -> set:
        return {node.job for node in cls.__registry}

    @classmethod
    def get_leafs(cls) -> set:
        all_nodes = set(cls.__registry)
        parents = cls._get_parents()
        return all_nodes - parents

    @classmethod
    def get_node(cls, job: 'Job') -> Optional['JobTree']:
        """
        Получаем узел дерева соответствующий задаче job
        """
        flt = filter(lambda x: x.job == job, cls.__registry)
        try:
            node = next(flt)
        except StopIteration:
            node = None
        return node
