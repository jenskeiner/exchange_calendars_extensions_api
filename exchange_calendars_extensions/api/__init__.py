__version__ = None

try:
    from importlib.metadata import version
    # get version from installed package
    __version__ = version("exchange_calendars_extensions")
    del version
except ImportError:
    pass

if __version__ is None:
    try:
        # if package not installed, get version as set when package built.
        from .version import version
    except Exception:
        # If package not installed and not built, leave __version__ as None
        pass
    else:
        __version__ = version
        del version
