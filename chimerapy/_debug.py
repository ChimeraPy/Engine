
def print_when_execute(func):

    def wrapper():
        print(func.__name__)
        output = func()
        return output

    return wrapper

