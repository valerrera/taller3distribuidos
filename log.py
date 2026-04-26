"""Helper de logging compartido. Imprime con timestamp y flush inmediato."""

import sys
import time


def log(msg, comp=""):
    ts = time.strftime("%H:%M:%S")
    prefix = f"[{ts}] [{comp}] " if comp else f"[{ts}] "
    print(prefix + msg, flush=True)


def banner(msg, comp="", char="="):
    line = char * 70
    log(line, comp)
    log(msg, comp)
    log(line, comp)
