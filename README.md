# exchange-calendars-extensions-api
This package contains parts of the API of 
[exchange-calendars-extensions](https://pypi.org/project/exchange-calendars-extensions/).

To separate the API from the implementation, the API is defined in a separate distribution package 
[exchange-calendars-extensions-api](https://pypi.org/project/exchange-calendars-extensions_api/), i.e. this package, 
that contains the sub-package `exchange_calendars_extensions.api`.

The implementation has been moved to the sub-package `exchange_calendars_extensions.core` and is distributed via 
[exchange-calendars-extensions](https://pypi.org/project/exchange-calendars-extensions/).

The package `exchange_calendars_extensions` is now a namespace package that wraps the sub-packages.
