"""Base event API."""

from collections import defaultdict

_registrars = defaultdict(list)


def _is_event_name(name):
    return not name.startswith('_') and name != 'dispatch'


class _UnpickleDispatch(object):

    """Serializable callable that re-generates an instance of :class:`_Dispatch`
    given a particular :class:`.Events` subclass.

    """

    def __call__(self, _parent_cls):
        for cls in _parent_cls.__mro__:
            if 'dispatch' in cls.__dict__:
                return cls.__dict__['dispatch'].dispatch_cls(_parent_cls)
        else:
            raise AttributeError("No class with a 'dispatch' member present.")


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

    def __reduce__(self):
        return _UnpickleDispatch(), (self._parent_cls, )

    def _update(self, other, only_propagate=True):
        """Populate from the listeners in another :class:`_Dispatch`
            object."""

        for ls in _event_descriptors(other):
            getattr(self, ls.name)._update(ls, only_propagate=only_propagate)


def _event_descriptors(target):
    return [getattr(target, k) for k in dir(target) if _is_event_name(k)]


def _remove_dispatcher(cls):
    for k in dir(cls):
        if _is_event_name(k):
            _registrars[k].remove(cls)
            if not _registrars[k]:
                del _registrars[k]


class _DispatchDescriptor(object):

    """Class-level attributes on :class:`._Dispatch` classes."""

    def __init__(self, fn):
        self.__name__ = fn.__name__
        self.__doc__ = fn.__doc__
        self._clslevel = defaultdict(list)

    def insert(self, obj, target, propagate):
        assert isinstance(target, type), \
            "Class-level Event targets must be classes."

        stack = [target]
        while stack:
            cls = stack.pop(0)
            stack.extend(cls.__subclasses__())
            self._clslevel[cls].insert(0, obj)

    def append(self, obj, target, propagate):
        assert isinstance(target, type), \
            "Class-level Event targets must be classes."

        stack = [target]
        while stack:
            cls = stack.pop(0)
            stack.extend(cls.__subclasses__())
            self._clslevel[cls].append(obj)

    def remove(self, obj, target):
        stack = [target]
        while stack:
            cls = stack.pop(0)
            stack.extend(cls.__subclasses__())
            self._clslevel[cls].remove(obj)

    def clear(self):
        """Clear all class level listeners"""

        for dispatcher in self._clslevel.values():
            dispatcher[:] = []

    def __get__(self, obj, cls):
        if obj is None:
            return self
        obj.__dict__[self.__name__] = result = \
            _ListenerCollection(self, obj._parent_cls)
        return result


class _ListenerCollection(object):

    """Instance-level attributes on instances of :class:`._Dispatch`.

    Represents a collection of listeners.

    """

    _exec_once = False

    def __init__(self, parent, target_cls):
        self.parent_listeners = parent._clslevel[target_cls]
        self.name = parent.__name__
        self.listeners = []
        self.propagate = set()

    def exec_once(self, *args, **kw):
        """Execute this event, but only if it has not been
        executed already for this collection."""

        if not self._exec_once:
            self(*args, **kw)
            self._exec_once = True

    def __call__(self, *args, **kw):
        """Execute this event."""

        for fn in self.parent_listeners:
            fn(*args, **kw)
        for fn in self.listeners:
            fn(*args, **kw)

    # I'm not entirely thrilled about the overhead here,
    # but this allows class-level listeners to be added
    # at any point.
    #
    # alternatively, _DispatchDescriptor could notify
    # all _ListenerCollection objects, but then we move
    # to a higher memory model, i.e.weakrefs to all _ListenerCollection
    # objects, the _DispatchDescriptor collection repeated
    # for all instances.

    def __len__(self):
        return len(self.parent_listeners + self.listeners)

    def __iter__(self):
        return iter(self.parent_listeners + self.listeners)

    def __getitem__(self, index):
        return (self.parent_listeners + self.listeners)[index]

    def __nonzero__(self):
        return bool(self.listeners or self.parent_listeners)

    def _update(self, other, only_propagate=True):
        """Populate from the listeners in another :class:`_Dispatch`
            object."""

        existing_listeners = self.listeners
        existing_listener_set = set(existing_listeners)
        self.propagate.update(other.propagate)
        existing_listeners.extend([l for l
                                   in other.listeners
                                   if l not in existing_listener_set
                                   and not only_propagate or l in self.propagate
                                   ])

    def insert(self, obj, target, propagate):
        if obj not in self.listeners:
            self.listeners.insert(0, obj)
            if propagate:
                self.propagate.add(obj)

    def append(self, obj, target, propagate):
        if obj not in self.listeners:
            self.listeners.append(obj)
            if propagate:
                self.propagate.add(obj)

    def remove(self, obj, target):
        if obj in self.listeners:
            self.listeners.remove(obj)
            self.propagate.discard(obj)

    def clear(self):
        self.listeners[:] = []
        self.propagate.clear()


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
                setattr(dispatch_cls, k, _DispatchDescriptor(dict_[k]))
                _registrars[k].append(cls)

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
