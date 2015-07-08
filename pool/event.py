"""Base event API."""

from collections import defaultdict


def _is_event_name(name):
    return not name.startswith('_') and name != 'dispatch'


class _Dispatch(object):

    """Mirror the event listening definitions of an Events class with
    listener collections.

    Classes which define a "dispatch" member will return a
    non-instantiated :class:`._Dispatch` subclass when the member
    is accessed at the class level.  When the "dispatch" member is
    accessed at the instance level of its owner, an instance
    of the :class:`._Dispatch` class is returned.

    A :class:`._Dispatch` class is generated for each :class:`.Events`
    class defined, by the :func:`._create_dispatcher_class` function.
    The original :class:`.Events` classes remain untouched.
    This decouples the construction of :class:`.Events` subclasses from
    the implementation used by the event internals, and allows
    inspecting tools like Sphinx to work in an unsurprising
    way against the public API.

    """

    def __init__(self, _parent_cls):
        self._parent_cls = _parent_cls

    def _update(self, other, only_propagate=True):
        """Populate from the listeners in another :class:`_Dispatch`
            object."""

        for ls in _event_descriptors(other):
            getattr(self, ls.name)._update(ls, only_propagate=only_propagate)


def _event_descriptors(target):
    return [getattr(target, k) for k in dir(target) if _is_event_name(k)]


class _EventMeta(type):

    """Intercept new Event subclasses and create
    associated _Dispatch classes."""

    def __init__(cls, classname, bases, dict_):
        """Create a :class:`._Dispatch` class corresponding to an
        :class:`.Events` class."""

        # there's all kinds of ways to do this,
        # i.e. make a Dispatch class that shares the '_listen' method
        # of the Event class, this is the straight monkeypatch.

        dispatch_base = getattr(cls, 'dispatch', _Dispatch)
        cls.dispatch = dispatch_cls = type("%sDispatch" % classname,
                                           (dispatch_base, ), {})
        for k in dict_:
            if _is_event_name(k):
                setattr(dispatch_cls, k, lambda *args, **kwargs: None)

        return type.__init__(cls, classname, bases, dict_)


class PoolEvents(object):

    """Define event listening functions for a particular target type.

    Available events for :class:`.Pool`.

    The methods here define the name of an event as well
    as the names of members that are passed to listener
    functions.

    e.g.::

        from sqlalchemy import event

        def my_on_checkout(dbapi_conn, connection_rec, connection_proxy):
            "handle an on checkout event"

        event.listen(Pool, 'checkout', my_on_checkout)

    In addition to accepting the :class:`.Pool` class and :class:`.Pool` instances,
    :class:`.PoolEvents` also accepts :class:`.Engine` objects and
    the :class:`.Engine` class as targets, which will be resolved
    to the ``.pool`` attribute of the given engine or the :class:`.Pool`
    class::

        engine = create_engine("postgresql://scott:tiger@localhost/test")

        # will associate with engine.pool
        event.listen(engine, 'checkout', my_on_checkout)

    """

    __metaclass__ = _EventMeta

    def connect(self, dbapi_connection, connection_record):
        """Called once for each new DB-API connection or Pool's ``creator()``.

        :param dbapi_con:
          A newly connected raw DB-API connection (not a SQLAlchemy
          ``Connection`` wrapper).

        :param con_record:
          The ``_ConnectionRecord`` that persistently manages the connection

        """

    def checkout(self, dbapi_connection, connection_record, connection_proxy):
        """Called when a connection is retrieved from the Pool.

        :param dbapi_con:
          A raw DB-API connection

        :param con_record:
          The ``_ConnectionRecord`` that persistently manages the connection

        :param con_proxy:
          The ``_ConnectionFairy`` which manages the connection for the span of
          the current checkout.

        If you raise a :class:`~sqlalchemy.exc.DisconnectionError`, the current
        connection will be disposed and a fresh connection retrieved.
        Processing of all checkout listeners will abort and restart
        using the new connection.
        """

    def checkin(self, dbapi_connection, connection_record):
        """Called when a connection returns to the pool.

        Note that the connection may be closed, and may be None if the
        connection has been invalidated.  ``checkin`` will not be called
        for detached connections.  (They do not return to the pool.)

        :param dbapi_con:
          A raw DB-API connection

        :param con_record:
          The ``_ConnectionRecord`` that persistently manages the connection

        """

    def first_connect(self, dbapi_connection, connection_record):
        """Called exactly once for the first DB-API connection.

        :param dbapi_con:
          A newly connected raw DB-API connection (not a SQLAlchemy
          ``Connection`` wrapper).

        :param con_record:
          The ``_ConnectionRecord`` that persistently manages the connection

        """
