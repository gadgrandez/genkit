# Copyright (c) 2001-2024 Python Software Foundation; All Rights Reserved
#
# This module is a backport of the `tasks.py` module from the Python standard
# library (originally from Python version 3.11) for use with Python 3.10.
# Licensed under the Python Software Foundation License Version 2.0 (PSF-2.0)
# https://docs.python.org/3/license.html
#
# SPDX-License-Identifier: PSF-2.0
#
# See: https://github.com/python/cpython/blob/3.11/Lib/asyncio/tasks.py
#
# Summary of changes made during backport:
# - Add type hints.
# - Added license header and SPDX identifier.
# - Change the order of appearance of the function definitions.
# - Docstrings added to _release_waiter.
# - Imports replaced with public imports.
# - Added wait_for_311 function based on the implementation in Python 3.11.
# - Added wait_for function that uses wait_for_311 for Python 3.10 and
#   asyncio.wait_for for Python 3.11 and later.
# - Changed the implementation of wait_for to raise TimeoutError instead of
#   exceptions.TimeoutError.
#
# Backported by: Google LLC.
# The full license text can be found in the LICENSE file.

"""Backport of asyncio.tasks module from Python 3.11 for use with Python 3.10."""

import asyncio
import functools
import sys
from asyncio import AbstractEventLoop, Future, ensure_future, events, exceptions
from typing import Any, TypeVar

T = TypeVar('T')


def _release_waiter(waiter: Future[Any], *args: Any) -> None:
    """Release the waiter."""
    if not waiter.done():
        waiter.set_result(None)


async def _cancel_and_wait(fut: Future[Any], loop: AbstractEventLoop) -> None:
    """Cancel the *fut* future or task and wait until it completes."""
    waiter = loop.create_future()
    cb = functools.partial(_release_waiter, waiter)
    fut.add_done_callback(cb)

    try:
        fut.cancel()
        # We cannot wait on *fut* directly to make
        # sure _cancel_and_wait itself is reliably cancellable.
        await waiter
    finally:
        fut.remove_done_callback(cb)


async def wait_for_311(fut: Future[T], timeout: float | None) -> T:
    """Wait for the single Future or coroutine to complete, with timeout.

    Coroutine will be wrapped in Task.

    Returns result of the Future or coroutine.  When a timeout occurs,
    it cancels the task and raises TimeoutError.  To avoid the task
    cancellation, wrap it in shield().

    If the wait is cancelled, the task is also cancelled.

    This function is a coroutine.
    """
    loop = events.get_running_loop()

    if timeout is None:
        return await fut

    if timeout <= 0:
        fut = ensure_future(fut, loop=loop)

        if fut.done():
            return fut.result()

        await _cancel_and_wait(fut, loop=loop)
        try:
            return fut.result()
        except exceptions.CancelledError as exc:
            # Original code:
            # raise exceptions.TimeoutError() from exc
            raise TimeoutError() from exc

    waiter = loop.create_future()
    timeout_handle = loop.call_later(timeout, _release_waiter, waiter)
    cb = functools.partial(_release_waiter, waiter)

    fut = ensure_future(fut, loop=loop)
    fut.add_done_callback(cb)

    try:
        # wait until the future completes or the timeout
        try:
            await waiter
        except exceptions.CancelledError:
            if fut.done():
                return fut.result()
            else:
                fut.remove_done_callback(cb)
                # We must ensure that the task is not running
                # after wait_for() returns.
                # See https://bugs.python.org/issue32751
                await _cancel_and_wait(fut, loop=loop)
                raise

        if fut.done():
            return fut.result()
        else:
            fut.remove_done_callback(cb)
            # We must ensure that the task is not running
            # after wait_for() returns.
            # See https://bugs.python.org/issue32751
            await _cancel_and_wait(fut, loop=loop)
            # In case task cancellation failed with some
            # exception, we should re-raise it
            # See https://bugs.python.org/issue40607
            try:
                return fut.result()
            except exceptions.CancelledError as exc:
                # Original code:
                # raise exceptions.TimeoutError() from exc
                raise TimeoutError() from exc
    finally:
        timeout_handle.cancel()


if sys.version_info < (3, 11):
    wait_for = wait_for_311
else:
    wait_for = asyncio.wait_for
