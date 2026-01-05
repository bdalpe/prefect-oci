import logging


class LoggerWriter:
    def __init__(self, logger: logging.Logger, level: int):
        self.logger = logger
        self.level = level
        self._buf = ""

    def write(self, message):
        if not message:
            return 0

        if isinstance(message, (bytes, bytearray)):
            message = message.decode(errors="replace")

        self._buf += message
        *lines, self._buf = self._buf.split("\n")

        for line in lines:
            if line.rstrip():
                self.logger.log(self.level, line.rstrip())

        return len(message)

    def flush(self):
        pass

    def close(self):
        if self._buf.strip():
            self.logger.log(self.level, self._buf.strip())
        self._buf = ""