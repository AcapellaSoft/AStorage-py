class KvError(Exception):
    pass


class CasError(KvError):
    pass


class KeyNotFoundError(KvError):
    pass


class TreeNotFoundError(KvError):
    pass


class TransactionNotFoundError(KvError):
    pass


class TransactionCompletedError(KvError):
    pass


class AuthenticationFailedError(KvError):
    pass
