import dataclasses_json
from dataclasses import dataclass, fields, is_dataclass
from typing import Any, TypeVar, Optional

from .eventbus import EventBus, Event
from .observables import ObservableDict, ObservableList

T = TypeVar("T")

# Global variables
global_event_bus: Optional["EventBus"] = None


@dataclass
class DataClassEvent:
    dataclass: Any


def configure(event_bus: EventBus):
    global global_event_bus
    global_event_bus = event_bus


def evented(cls):
    original_init = cls.__init__

    def new_init(self, *args, **kwargs):
        global global_event_bus

        self.event_bus = None

        if isinstance(global_event_bus, EventBus):
            self.event_bus = global_event_bus

        original_init(self, *args, **kwargs)

    def make_property(name: str) -> Any:
        def getter(self):
            return self.__dict__[f"_{name}"]

        def setter(self, value):
            self.__dict__[f"_{name}"] = value
            if self.event_bus:
                event_name = f"{cls.__name__}.changed"
                event_data = DataClassEvent(self)
                self.event_bus.send(Event(event_name, event_data))

        return property(getter, setter)

    cls.__init__ = new_init

    for f in fields(cls):
        if f.name != "event_bus":
            setattr(cls, f.name, make_property(f.name))

    setattr(
        cls,
        "event_bus",
        dataclasses_json.config(
            field_name="event_bus", encoder=lambda x: None, decoder=lambda x: None
        ),
    )

    return cls


def make_evented(
    instance: T,
    event_bus: "EventBus",
    event_name: Optional[str] = None,
    object: Optional[Any] = None,
) -> T:
    setattr(instance, "event_bus", event_bus)
    instance.__evented_values = {}  # type: ignore[attr-defined]

    # Name of the event
    if not event_name:
        event_name = f"{instance.__class__.__name__}.changed"

    # Dynamically create a new class with the same name as the instance's class
    new_class_name = instance.__class__.__name__
    NewClass = type(new_class_name, (instance.__class__,), {})

    def make_property(name: str):
        def getter(self):
            return self.__evented_values.get(name)

        def setter(self, value):
            self.__evented_values[name] = value
            if object:
                event_data = DataClassEvent(object)
            else:
                event_data = DataClassEvent(self)

            event_bus.send(Event(event_name, event_data))

        return property(getter, setter)

    def callback(key, value):
        if object:
            event_data = DataClassEvent(object)
        else:
            event_data = DataClassEvent(instance)

        event_bus.send(Event(event_name, event_data))

    for f in fields(instance.__class__):
        if f.name != "event_bus":
            attr_value = getattr(instance, f.name)

            # Check if other dataclass
            if is_dataclass(attr_value):
                attr_value = make_evented(attr_value, event_bus, event_name, instance)

            # If the attribute is a dictionary, replace it with an ObservableDict
            elif isinstance(attr_value, dict):
                attr_value = ObservableDict(attr_value)
                attr_value.set_callback(callback)

            # Handle list
            elif isinstance(attr_value, list):
                attr_value = ObservableList(attr_value)
                attr_value.set_callback(callback)

            instance.__evented_values[f.name] = attr_value  # type: ignore[attr-defined]
            setattr(NewClass, f.name, make_property(f.name))

    # Change the class of the instance
    instance.__class__ = NewClass

    return instance
