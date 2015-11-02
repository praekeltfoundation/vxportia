import json
from uuid import uuid4

from twisted.internet import reactor
from twisted.internet.defer import maybeDeferred, Deferred
from twisted.protocols.basic import LineReceiver
from twisted.python import log


class PortiaProtocolException(Exception):
    def __init__(self, message, data={}):
        self.message = message
        self.data = data


class PortiaProtocol(LineReceiver):

    version = "0.1.0"
    timeout = 10
    clock = reactor

    def __init__(self):
        self.queue = {}

    def force_timeout(self, reference_id):
        d, _ = self.queue.pop(reference_id, None)
        if d and not d.called:
            d.errback(PortiaProtocolException('Timeout exceeded.'))

    def send_command(self, cmd, reference_id=None, **kwargs):
        reference_id = reference_id or uuid4().hex
        data = {
            "cmd": cmd,
            "version": "0.1.0",
            "id": reference_id,
            "version": self.version,
            "request": kwargs,
        }
        d = Deferred()
        assassin = self.clock.callLater(
            self.timeout, self.force_timeout, reference_id)
        self.queue[reference_id] = (d, assassin)
        self.sendLine(json.dumps(data))
        return d

    def lineReceived(self, line):
        d = maybeDeferred(self.parseLine, line)
        d.addErrback(log.err)

    def parseLine(self, line):
        data = json.loads(line)
        status = data['status']
        reference_id = data['reference_id']
        reference = self.queue.pop(reference_id, None)

        if reference is None:
            raise PortiaProtocolException(data)

        d, assassin = reference
        assassin.cancel()
        if status == 'ok':
            d.callback(data['response'])
        else:
            d.errback(PortiaProtocolException(data['message'], data))

    def get(self, msisdn):
        return self.send_command('get', msisdn=msisdn)

    def resolve(self, msisdn):
        return self.send_command('resolve', msisdn=msisdn)

    def annotate(self, msisdn, key, value, timestamp=None):
        return self.send_command(
            'annotate', msisdn=msisdn, key=key, value=value,
            timestamp=(timestamp.isoformat() if timestamp else None))
