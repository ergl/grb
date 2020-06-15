grb
===========

A riak_core application

Build
-----

::

    rebar3 release

Test
----

::

    rebar3 ct

Run
---

::

    ./_build/default/rel/grb/bin/grb console

Try
---

::

    1> grb:ping().
    {pong,'grb1@127.0.0.1', 9...8}

Quit
----

::

    2> q().

More information:

* `Getting Started Guide <https://riak-core-lite.github.io/blog/pages/getting-started/>`_
* `Riak Core Lite Site <https://riak-core-lite.github.io/>`_

TODO
----

* define license and create LICENSE file

License
-------

TODO
