from Crypto.PublicKey import RSA
from base64 import b64encode, b64decode


class RSAKey(object):
    def __init__(self, _key, used_load=False):
        if not used_load:
            raise ValueError("Use load_key method")

        self._key = _key

    def __str__(self):

        return self.string()


    @staticmethod
    def read(_key):
        if type(_key) == str:
            key = RSA.importKey(b64decode(_key))
            _key = key.exportKey('PEM')
        return RSAKey(_key, used_load=True)


    def string(self):
        return b64encode(self._key).decode()
    def bytes(self):
        return self._key

