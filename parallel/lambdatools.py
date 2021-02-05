import sys
import types
import typing


def is_lambda_function(func: typing.Callable) -> bool:
    return isinstance(func, types.LambdaType) and func.__name__ == "<lambda>"


class LambdaP:
    def __init__(self, lambda_: typing.Callable):
        self._lambda = None
        self._lambda_code_attrs = self.get_lambda_code_attrs(lambda_)

    @staticmethod
    def get_lambda_code_attrs(lambda_: typing.Callable) -> tuple:
        co = lambda_.__code__

        if sys.version_info[1] >= 8:
            return (
                co.co_argcount,
                co.co_posonlyargcount,
                co.co_kwonlyargcount,
                co.co_nlocals,
                co.co_stacksize,
                co.co_flags,
                co.co_code,
                co.co_consts,
                co.co_names,
                co.co_varnames,
                co.co_filename,
                lambda_.__name__,
                co.co_firstlineno,
                co.co_lnotab,
                co.co_freevars,
                co.co_cellvars
            )
        else:
            return (
                co.co_argcount,
                co.co_kwonlyargcount,
                co.co_nlocals,
                co.co_stacksize,
                co.co_flags,
                co.co_code,
                co.co_consts,
                co.co_names,
                co.co_varnames,
                co.co_filename,
                lambda_.__name__,
                co.co_firstlineno,
                co.co_lnotab,
                co.co_freevars,
                co.co_cellvars
            )

    @staticmethod
    def build_lambda(code_attrs: tuple) -> typing.Callable:
        lambda_obj = lambda: None
        lambda_obj.__code__ = types.CodeType(*code_attrs)
        return lambda_obj

    def __getstate__(self):
        return self._lambda_code_attrs

    def __setstate__(self, code_attrs):
        self.__dict__.update(
            {
                '_lambda': None,
                '_lambda_code_attrs': code_attrs,
            }
        )

    def __call__(self, *args, **kwargs):
        if self._lambda is None:
            self._lambda = self.build_lambda(self._lambda_code_attrs)

        return self._lambda(*args, **kwargs)


def prepare_func(func: typing.Callable) -> typing.Callable:
    return LambdaP(func) if is_lambda_function(func) else func
