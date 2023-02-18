import logging
from enum import IntEnum
from typing import Set, Optional, List, Union

logging.basicConfig(
    level='INFO',
    # filename='application-log.log',
    # filemode='w',
    format='%(asctime)s %(name)-30s %(levelname)-8s %(message)s'
)

WORK_DIR_NAME = 'work-dir'
DUMP_FILE_NAME = 'dump.json'


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
    CREATE = 0          # Создание
    DELETE_DIR = 1      # Удаление директории
    DELETE_FILE = 2     # Удаление файла
    RENAME = 3          # Переименование


def extract_list_of_lists(lst: List[List]) -> List:
    """
    Распаковываем список списков
    :param lst: список списков
    :return: распакованный список
    """
    return [j for i in lst for j in i]


class JobTree:
    """
    Дерево задач
    """
    __registry = []

    def __init__(self, job: 'Job'):
        assert job not in JobTree.all_jobs(), 'Этот элемент уже добавлен'
        self.descendants = []
        self.job = job
        self.__registry.append(self)

    def __str__(self) -> str:
        return f'Узел для задачи {self.job}'

    def __repr__(self) -> str:
        return self.__str__()

    def set_relations(self, node: 'JobTree') -> None:
        """
        Устанавливаем связь текущего узла с узлом node
        :param node: родитель текущего узла
        :return:
        """
        node.descendants.append(self)
        # self.parents.append(node)

    def get_descendants(self) -> Set['JobTree']:
        """
        Ищем всех предков для указанного узла + сам узел
        :return: Множество предков для узла + сам узел
        """
        descendants = []

        nodes = [[self], ]
        while True:
            for node_item in nodes:
                for node in node_item:
                    descendants.append(node.descendants)

            nodes = descendants[-len(nodes):]
            if not any(nodes):
                break

        descendants.append([self, ])

        return set(extract_list_of_lists(descendants))

    @classmethod
    def del_nodes(cls, nodes: Set['JobTree']) -> List['Job']:
        deleted_jobs = []
        for node in nodes:
            job = node.job
            cls.delete(job)
            deleted_jobs.append(job)
        return deleted_jobs

    @classmethod
    def delete(cls, job: 'Job'):
        try:
            node = cls.get_node(job)
            cls.__registry.remove(node)
        except ValueError:
            raise NodeDeleteException

    @classmethod
    def _get_all_descendants(cls) -> set:
        descendants = []
        for node in cls.__registry:
            descendants.extend(node.descendants)
        return set(descendants)

    @classmethod
    def all_nodes(cls) -> list:
        return cls.__registry

    @staticmethod
    def get_jobs_from_nodes(nodes: Union[List['JobTree'], Set['JobTree']]) -> Set['Job']:
        return {node.job for node in nodes}

    @classmethod
    def all_jobs(cls) -> Set['Job']:
        return cls.get_jobs_from_nodes(cls.__registry)

    @classmethod
    def get_leafs(cls) -> set:
        all_nodes = set(cls.__registry)
        descendants = cls._get_all_descendants()
        return all_nodes - descendants

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
