from typing import List

import pytest

from deirokay.backend import MultiBackendMixin, register_backend_method
from deirokay.enums import Backend


class BaseClassForTests(MultiBackendMixin):
    def __init__(self, name) -> None:
        self.name = name


def test_non_multibackend_subclass():
    with pytest.raises(RuntimeError):
        class NotAMultibackendMiximSubclass():
            @register_backend_method('report', Backend.PANDAS)
            def _report(self):
                ...


class OneBackend(BaseClassForTests):
    supported_backends: List[Backend] = [Backend.PANDAS]

    @register_backend_method('report', Backend.PANDAS)
    def _report(self):
        return self.name, Backend.PANDAS

    @register_backend_method('profile', Backend.PANDAS)
    @staticmethod
    def _profile():
        return 'ok', Backend.PANDAS


class TwoBackends(BaseClassForTests):
    supported_backends: List[Backend] = [Backend.PANDAS, Backend.DASK]

    @register_backend_method('report', Backend.DASK)
    def _report_dask(self):
        return self.name, Backend.DASK

    @register_backend_method('report', Backend.PANDAS)
    def _report_pandas(self):
        return self.name, Backend.PANDAS

    @register_backend_method('profile', Backend.DASK)
    @staticmethod
    def _profile_dask():
        return 'ok', Backend.DASK

    @register_backend_method('profile', Backend.PANDAS)
    @staticmethod
    def _profile_pandas():
        return 'ok', Backend.PANDAS


def test_use_valid_backend():
    GeneratedClassA = OneBackend.attach_backend(Backend.PANDAS)
    assert hasattr(GeneratedClassA, 'report')
    assert GeneratedClassA('tvalue').report() == ('tvalue', Backend.PANDAS)
    assert GeneratedClassA.profile() == ('ok', Backend.PANDAS)
    assert GeneratedClassA('tvalue').profile() == ('ok', Backend.PANDAS)

    GeneratedClassB1 = TwoBackends.attach_backend(Backend.PANDAS)
    assert hasattr(GeneratedClassB1, 'report')
    assert GeneratedClassB1('tvalue').report() == ('tvalue', Backend.PANDAS)
    assert GeneratedClassB1.profile() == ('ok', Backend.PANDAS)
    assert GeneratedClassB1('tvalue').profile() == ('ok', Backend.PANDAS)

    GeneratedClassB2 = TwoBackends.attach_backend(Backend.DASK)
    assert hasattr(GeneratedClassB2, 'report')
    assert GeneratedClassB2('tvalue').report() == ('tvalue', Backend.DASK)
    assert GeneratedClassB2.profile() == ('ok', Backend.DASK)
    assert GeneratedClassB2('tvalue').profile() == ('ok', Backend.DASK)


def test_use_unsupported_backend():
    GeneratedClassA = OneBackend.attach_backend(Backend.PANDAS)
    assert hasattr(GeneratedClassA, 'report')
    with pytest.raises(AssertionError):
        assert GeneratedClassA('tvalue').report() == ('tvalue', Backend.DASK)
    with pytest.raises(AssertionError):
        assert GeneratedClassA.profile() == ('ok', Backend.DASK)
    with pytest.raises(AssertionError):
        assert GeneratedClassA('tvalue').profile() == ('ok', Backend.DASK)


def test_two_different_backends_in_a_row():
    GeneratedClassA1 = TwoBackends.attach_backend(Backend.PANDAS)
    GeneratedClassA2 = TwoBackends.attach_backend(Backend.DASK)

    assert hasattr(GeneratedClassA1, 'report')
    assert GeneratedClassA1('tvalue').report() == ('tvalue', Backend.PANDAS)
    assert GeneratedClassA1.profile() == ('ok', Backend.PANDAS)
    assert GeneratedClassA1('tvalue').profile() == ('ok', Backend.PANDAS)

    assert hasattr(GeneratedClassA2, 'report')
    assert GeneratedClassA2('tvalue').report() == ('tvalue', Backend.DASK)
    assert GeneratedClassA2.profile() == ('ok', Backend.DASK)
    assert GeneratedClassA2('tvalue').profile() == ('ok', Backend.DASK)


class SimpleExtensionSubclass(TwoBackends):
    ...


def test_simple_subclass_of_multibackend_class():
    GeneratedClassA = SimpleExtensionSubclass.attach_backend(Backend.DASK)
    assert hasattr(GeneratedClassA, 'report')
    assert GeneratedClassA('tvalue').report() == ('tvalue', Backend.DASK)
    assert GeneratedClassA.profile() == ('ok', Backend.DASK)
    assert GeneratedClassA('tvalue').profile() == ('ok', Backend.DASK)


class ExtendAClassWithNewBackend(OneBackend):
    supported_backends = OneBackend.supported_backends + [Backend.DASK]

    @register_backend_method('report', Backend.DASK)
    def _report_dask(self):
        return self.name, Backend.DASK

    @register_backend_method('profile', Backend.DASK)
    @staticmethod
    def _profile_dask():
        return 'ok', Backend.DASK


def test_subclass_extends_with_new_backend():
    # Backend from parent
    GeneratedClassA1 = ExtendAClassWithNewBackend.attach_backend(
        Backend.PANDAS
    )
    assert hasattr(GeneratedClassA1, 'report')
    assert GeneratedClassA1('tvalue').report() == ('tvalue', Backend.PANDAS)
    assert GeneratedClassA1.profile() == ('ok', Backend.PANDAS)
    assert GeneratedClassA1('tvalue').profile() == ('ok', Backend.PANDAS)

    # New backend
    GeneratedClassA2 = ExtendAClassWithNewBackend.attach_backend(
        Backend.DASK
    )
    assert hasattr(GeneratedClassA2, 'report')
    assert GeneratedClassA2('tvalue').report() == ('tvalue', Backend.DASK)
    assert GeneratedClassA2.profile() == ('ok', Backend.DASK)
    assert GeneratedClassA2('tvalue').profile() == ('ok', Backend.DASK)


class ExtendAClassAndUseParentMethods(OneBackend):
    @register_backend_method('report', Backend.PANDAS)
    def _report(self):
        return super()._report()

    @register_backend_method('profile', Backend.PANDAS)
    @staticmethod
    def _profile():
        return OneBackend._profile()


def test_subclass_using_super():
    GeneratedClassA = ExtendAClassAndUseParentMethods.attach_backend(
        Backend.PANDAS
    )
    assert hasattr(GeneratedClassA, 'report')
    assert GeneratedClassA('tvalue').report() == ('tvalue', Backend.PANDAS)
    assert GeneratedClassA.profile() == ('ok', Backend.PANDAS)
    assert GeneratedClassA('tvalue').profile() == ('ok', Backend.PANDAS)
