import functools
from typing_extensions import ParamSpec
from typing import IO, Callable, Iterator, Optional

from .elements import Element

_P = ParamSpec("_P")


def add_metadata_with_filetype(
    filetype,
) -> Callable[[Callable[_P, list[Element]]], Callable[_P, list[Element]]]:
    """..."""

    def decorator(func: Callable[_P, list[Element]]) -> Callable[_P, list[Element]]:
        return add_filetype(filetype=filetype)(add_metadata(func))

    return decorator


def add_metadata(func: Callable[_P, list[Element]]) -> Callable[_P, list[Element]]:
    @functools.wraps(func)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> list[Element]:
        elements = func(*args, **kwargs)
        call_args = get_call_args_applying_defaults(func, *args, **kwargs)

        if call_args.get("metadata_filename"):
            call_args["filename"] = call_args.get("metadata_filename")

        metadata_kwargs = {
            kwarg: call_args.get(kwarg) for kwarg in ("filename", "url", "text_as_html")
        }
        # NOTE (yao): do not use cast here as cast(None) still is None
        if not str(kwargs.get("model_name", "")).startswith("chipper"):
            # NOTE(alan): Skip hierarchy if using chipper, as it should take care of that
            elements = set_element_hierarchy(elements)

        for element in elements:
            # NOTE(robinson) - Attached files have already run through this logic
            # in their own partitioning function
            if element.metadata.attached_to_filename is None:
                add_element_metadata(element, **metadata_kwargs)

        return elements

    return wrapper


def add_filetype(
    filetype: FileType,
) -> Callable[[Callable[_P, list[Element]]], Callable[_P, list[Element]]]:
    """Post-process element-metadata for list[Element] from partitioning.

    This decorator adds a post-processing step to a document partitioner.

    - Adds `.metadata.filetype` (source-document MIME-type) metadata value

    This "partial" decorator is present because `partition_image()` does not apply
    `.metadata.filetype` this way since each image type has its own MIME-type (e.g. `image.jpeg`,
    `image/png`, etc.).
    """

    def decorator(func: Callable[_P, list[Element]]) -> Callable[_P, list[Element]]:
        @functools.wraps(func)
        def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> list[Element]:
            elements = func(*args, **kwargs)

            for element in elements:
                # NOTE(robinson) - Attached files have already run through this logic
                # in their own partitioning function
                if element.metadata.attached_to_filename is None:
                    add_element_metadata(element, filetype=filetype.mime_type)

            return elements

        return wrapper

    return decorator

