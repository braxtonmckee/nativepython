import threading


class IdentityProducer:
    def __init__(self, ix):
        self.ix = ix
        self.count = 0
        self.lock = threading.Lock()

    def createIdentity(self):
        with self.lock:
            count = self.count
            self.count += 1

        ONE_HUNDRED_MILLION = 100000000
        return self.ix * ONE_HUNDRED_MILLION + count