from abc import abstractmethod, ABC


class Observer(ABC):
    @abstractmethod
    def update(self, message) -> None:
        ...


class Subject(ABC):

    @abstractmethod
    def attach(self, observer: Observer) -> None:
        ...

    @abstractmethod
    def detach(self, observer: Observer) -> None:
        ...

    @abstractmethod
    def notify(self, message) -> None:
        ...
