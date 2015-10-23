import pkg_resources
import json

from twisted.trial.unittest import TestCase
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, Deferred
from twisted.internet.protocol import Factory
from twisted.internet.task import Clock
from twisted.test.proto_helpers import StringTransportWithDisconnection

from vxportia.protocol import PortiaProtocol, PortiaProtocolException

from portia.portia import Portia
from portia.utils import (
    start_redis, compile_network_prefix_mappings)


class TestPortiaProtocol(TestCase):

    timeout = 1

    @inlineCallbacks
    def setUp(self):
        self.redis = yield start_redis()
        self.addCleanup(self.redis.disconnect)

        self.portia = Portia(
            self.redis,
            network_prefix_mapping=compile_network_prefix_mappings(
                [pkg_resources.resource_filename(
                    'portia', 'assets/mappings/*.mapping.json')]))
        self.addCleanup(self.portia.flush)

        factory = Factory.forProtocol(PortiaProtocol)
        self.proto = factory.buildProtocol(None)
        self.proto.clock = Clock()
        self.transport = StringTransportWithDisconnection()
        self.proto.makeConnection(self.transport)

    def read_command(self):
        response_d = Deferred()

        def check(d):
            val = self.transport.value()
            self.transport.clear()
            if val:
                d.callback(json.loads(val))
                return
            reactor.callLater(0, check, d)
        reactor.callLater(0, check, response_d)

        return response_d

    def reply(self, command, response={}, status='ok', message=None):
        self.proto.parseLine(json.dumps({
            'status': status,
            'cmd': 'reply',
            'reference_cmd': command['cmd'],
            'reference_id': command['id'],
            'version': command['version'],
            'response': response,
            'message': message,
        }))

    @inlineCallbacks
    def test_get(self):
        d = self.proto.get('27123456789')
        command = yield self.read_command()
        self.assertEqual(command['request'], {
            'msisdn': '27123456789',
        })
        self.reply(command, {
            'observed-network': 'Foo',
            'observed-network-timestamp': self.portia.now().isoformat(),
        })
        response = yield d
        self.assertEqual(response['observed-network'], 'Foo')
        self.assertEqual(self.proto.queue, {})

    @inlineCallbacks
    def test_get_fail(self):
        d = self.proto.get('27123456789')
        command = yield self.read_command()
        self.assertEqual(command['request'], {
            'msisdn': '27123456789',
        })
        self.reply(command, status='error', message='something failed')
        f = yield self.assertFailure(d, PortiaProtocolException)
        self.assertEqual(f.message, 'something failed')
        self.assertEqual(f.data['status'], 'error')
        self.assertEqual(f.data['reference_cmd'], 'get')
        self.assertEqual(f.data['reference_id'], command['id'])
        self.assertEqual(self.proto.queue, {})

    @inlineCallbacks
    def test_timeout(self):
        d = self.proto.get('27123456789')
        self.proto.clock.advance(self.proto.timeout + 1)
        f = yield self.assertFailure(d, PortiaProtocolException)
        self.assertEqual(f.message, 'Timeout exceeded.')
        self.assertEqual(self.proto.queue, {})

    @inlineCallbacks
    def test_resolve(self):
        d = self.proto.resolve('27123456789')
        command = yield self.read_command()
        self.assertEqual(command['cmd'], 'resolve')
        self.assertEqual(command['request'], {
            'msisdn': '27123456789',
        })

        self.reply(command, {
            "entry": {
                "ported-to": "CELLC",
                "ported-to-timestamp": "2015-10-16T19:26:41.943293",
                "observed-network": "MTN",
                "observed-network-timestamp": "2015-10-16T19:49:21.130930"
            },
            "network": "MTN",
            "strategy": "observed-network",
        })
        response = yield d
        self.assertEqual(response['network'], 'MTN')

    @inlineCallbacks
    def test_annotate(self):
        now = self.portia.now()
        d = self.proto.annotate('27123456789',
                                key='X-Key', value='value',
                                timestamp=now)
        command = yield self.read_command()
        self.assertEqual(command['cmd'], 'annotate')
        self.assertEqual(command['request'], {
            'msisdn': '27123456789',
            'key': 'X-Key',
            'value': 'value',
            'timestamp': now.isoformat(),
        })
        self.reply(command, "ok")
        response = yield d
        self.assertEqual(response, 'ok')
