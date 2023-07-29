class ObservableDict(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.callback = None  # initialize the callback

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        if self.callback:
            self.callback(key, value)

    def __delitem__(self, key):
        super().__delitem__(key)
        if self.callback:
            self.callback(key, None)

    def set_callback(self, callback):
        self.callback = callback


class ObservableList(list):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.callback = None  # initialize the callback

    def __setitem__(self, index, value):
        super().__setitem__(index, value)
        if self.callback:
            self.callback(index, value)

    def __delitem__(self, index):
        super().__delitem__(index)
        if self.callback:
            self.callback(index, None)

    def append(self, value):
        super().append(value)
        if self.callback:
            self.callback(len(self) - 1, value)

    def extend(self, iterable):
        start_len = len(self)
        super().extend(iterable)
        if self.callback:
            for i, item in enumerate(iterable, start=start_len):
                self.callback(i, item)

    def insert(self, index, value):
        super().insert(index, value)
        if self.callback:
            self.callback(index, value)

    def remove(self, value):
        index = self.index(value)
        super().remove(value)
        if self.callback:
            self.callback(index, None)

    def pop(self, index=-1):
        value = super().pop(index)
        if self.callback:
            self.callback(index, None)
        return value

    def set_callback(self, callback):
        self.callback = callback
