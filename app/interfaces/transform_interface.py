from abc import ABC, abstractmethod


class DataTransform:
    @abstractmethod
    def clean(self, df):
        pass
