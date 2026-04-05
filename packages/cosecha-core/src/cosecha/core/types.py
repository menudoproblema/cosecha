from types import TracebackType


type ExcInfo = (
    tuple[type[BaseException], BaseException, TracebackType]
    | tuple[None, None, None]
)
