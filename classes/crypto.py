import base64
import struct


class Rc4(object):
    def __init__(self, key, drop_n_bytes=768):
        self.ksa = self.make_key(key, drop_n_bytes)

    def encrypt_byte(self, i, j, S):
        i = (i + 1) % 256
        j = (j + S[i]) % 256
        S[i], S[j] = S[j], S[i]
        K = S[(S[i] + S[j]) % 256]
        return (i, j, K)

    def make_key(self, key, drop_n_bytes):
        # The key-scheduling algorithm (KSA)
        S = [i for i in range(256)]
        j = 0
        for i in range(256):
            j = (j + S[i] + ord(key[i % len(key)])) % 256
            S[i], S[j] = S[j], S[i]

        self.i = 0
        self.j = 0
        # Do the RC4-drop[(nbytes)]
        if drop_n_bytes:
            # i = j = 0
            for dropped in range(drop_n_bytes):
                self.i, self.j, K = self.encrypt_byte(self.i, self.j, S)
        return S

    def __crypt(self, message):
        # The pseudo-random generation algorithm (PRGA)
        S = list(self.ksa)  # make a deep copy of you KSA array, gets modified
        combined = []
        counter = 0
        i, j = self.i, self.j
        for c in message:
            i, j, K = self.encrypt_byte(i, j, S)
            combined.append(struct.pack('B', (K ^ ord(c))))

        crypted = (''.join(combined))
        return crypted

    def encode(self, message, encodeBase64=True):
        crypted = self.__crypt(message)
        if encodeBase64:
            crypted = base64.urlsafe_b64encode(crypted)
        return crypted

    def decode(self, message, encodedBase64=True):
        if encodedBase64:
            message = base64.urlsafe_b64decode(message.encode())
        return self.__crypt(message)
