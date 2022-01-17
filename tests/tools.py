import threading

def raise_err():
    raise Exception()
def no_err():
    return

class ThreadedTestRunner():

    def __init__(self):
        self.threads = {}
        self.thread_results = {}

    def add(self, target, name):
        self.threads[name] = threading.Thread(target = self.run, args = [target, name])
        self.threads[name].start()

    def run(self, target, name):
        self.thread_results[name] = 'fail'
        target()
        self.thread_results[name] = 'pass'

    # def check_result(self, name):
    #     self.threads[name].join()
    #     assert(self.thread_results[name] == 'pass')
