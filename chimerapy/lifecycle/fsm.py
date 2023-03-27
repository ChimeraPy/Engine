from typing import List, Optional, Union


class StateTransitionError(ValueError):
    pass


class FSMFinishedError(StateTransitionError):
    pass


class Transition:
    """A transition between two states in a finite state machine."""

    def __init__(self, name: str, from_state: str, to_state: str):
        self.name = name
        self.from_state = from_state
        self.to_state = to_state

    @property
    def key(self):
        return self.name

    def __repr__(self):
        return f"<Transition({self.name}): {self.from_state} -> {self.to_state}>"


class State:
    """A state in a finite state machine."""

    def __init__(
        self,
        name: str,
        valid_transitions: List[Transition],
        description: Optional[str] = None,
    ):
        self.name = name
        if not valid_transitions:
            valid_transitions = []
        self.valid_transitions = valid_transitions
        self.description = description or "A state in a finite state machine."

    def __repr__(self):
        return f"<State {self.name}, Valid Transitions: {[t.key + ' -> ' + t.to_state for t in self.valid_transitions]}>"

    def describe(self):
        return f"{self.name}: {self.description}\n{repr(self)}"


class FSM:
    """A finite state machine."""

    def __init__(
        self,
        states: List[State],
        initial_state: State,
        description: Optional[str] = None,
    ):
        self.states = states
        self.initial_state = initial_state
        self.current_state = initial_state
        self.description = description or "A finite state machine."
        final_states = set()

        for state in states:
            if state.valid_transitions is None or len(state.valid_transitions) == 0:
                final_states.add(state)

        self.final_states = frozenset(final_states)
        self.transitioning = False

    def transition(self, transition: Union[Transition, str]) -> None:
        if self.transitioning:
            raise StateTransitionError("Cannot transition while transitioning")

        if self.is_finished:
            raise FSMFinishedError(f"The final state reached")

        if isinstance(transition, str):
            transition = self.get_transition(transition)

        if not self.is_valid_transition(transition):
            raise StateTransitionError(
                f"Cannot transition from {self.current_state.name} to {transition.to_state.name}"
            )

        self.current_state = self._get_state_from_transition(transition)

    def _get_state_from_transition(self, transition: Transition) -> State:
        for state in self.states:
            if state.name == transition.to_state:
                return state

    def is_valid_transition(self, transition: Transition) -> bool:
        if transition is None:
            return False

        return transition in self.current_state.valid_transitions

    def get_transition(self, transition_name: str) -> Optional[Transition]:
        for transition in self.current_state.valid_transitions:
            if transition.key == transition_name:
                return transition
        return None

    @property
    def is_finished(self):
        return self.current_state in self.final_states

    def __repr__(self):
        return f"<FSM {self.current_state.name}>"

    @staticmethod
    def parse_dict(dict_obj):
        state_cache = {}
        for state_name, props in dict_obj["states"].items():
            state_cache[state_name] = State(
                name=state_name,
                valid_transitions=[
                    Transition(name=k, from_state=state_name, to_state=v)
                    for k, v in props["allowed_transitions"].items()
                ],
                description=props["description"],
            )

        return state_cache, state_cache[dict_obj["initial_state"]]

    @classmethod
    def from_dict(cls, dict_obj):
        state_cache, initial_state = cls.parse_dict(dict_obj)

        return cls(
            states=list(state_cache.values()),
            initial_state=state_cache[dict_obj["initial_state"]],
            description=dict_obj["description"],
        )
