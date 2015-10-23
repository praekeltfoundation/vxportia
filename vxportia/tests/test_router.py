import pkg_resources

from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import Clock

from vumi.errors import DispatcherError
from vumi.dispatchers.tests.helpers import DummyDispatcher
from vumi.tests.helpers import VumiTestCase, MessageHelper

from vxportia.router import PortiaRouter

from portia.portia import Portia
from portia.utils import (
    start_redis, start_tcpserver, compile_network_prefix_mappings)


class TestPortiaRouter(VumiTestCase):

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

        self.listener = yield start_tcpserver(self.portia, 'tcp:0')
        self.addCleanup(self.listener.loseConnection)

        self.listener_host = self.listener.getHost().host
        self.listener_port = self.listener.getHost().port
        self.config = {
            'portia': {
                'client_endpoint': 'tcp:%s:%s' % (self.listener_host,
                                                  self.listener_port),
                'default_transport': 'transport_3',
                'mappings': {
                    'MNO1': 'transport_1',
                    'MNO2': 'transport_2',
                },
            },
            'transport_names': ['transport_1', 'transport_2', 'transport_3'],
            'exposed_names': ['app'],
        }

        self.dispatcher = DummyDispatcher(self.config)
        self.router = PortiaRouter(self.dispatcher, self.config)
        self.add_cleanup(self.router.teardown_routing)
        yield self.router.setup_routing()
        self.router.portia.clock = Clock()
        self.msg_helper = self.add_helper(MessageHelper())

    @inlineCallbacks
    def test_dispatch_inbound_message(self):
        msg1 = self.msg_helper.make_inbound(
            "1", from_addr='27123456789', transport_name='transport_1')
        msg2 = self.msg_helper.make_inbound(
            "1", from_addr='27123456780', transport_name='transport_2')
        yield self.router.dispatch_inbound_message(msg1)
        yield self.router.dispatch_inbound_message(msg2)
        response1 = yield self.portia.resolve('27123456789')
        response2 = yield self.portia.resolve('27123456780')
        self.assertEqual(response1['network'], 'MNO1')
        self.assertEqual(response2['network'], 'MNO2')
        publishers = self.dispatcher.exposed_publisher
        self.assertEqual(publishers['app'].msgs, [msg1, msg2])

    @inlineCallbacks
    def test_dispatch_inbound_event(self):
        ack = self.msg_helper.make_ack(
            from_addr='27123456789', transport_name='transport_1')
        yield self.router.dispatch_inbound_event(ack)
        response = yield self.portia.resolve('27123456789')
        self.assertEqual(response['network'], None)
        publishers = self.dispatcher.exposed_event_publisher
        self.assertEqual(publishers['app'].msgs, [ack])

    @inlineCallbacks
    def test_dispatch_outbound_message(self):
        yield self.portia.annotate(
            '27123456789', key='observed-network', value='MNO1',
            timestamp=self.portia.now())
        msg = self.msg_helper.make_outbound("1", to_addr='27123456789')
        yield self.router.dispatch_outbound_message(msg)
        publishers = self.dispatcher.transport_publisher
        self.assertEqual(publishers['transport_1'].msgs, [msg])

    @inlineCallbacks
    def test_dispatch_default_transport(self):
        msg = self.msg_helper.make_outbound("1", to_addr='27123456789')
        yield self.router.dispatch_outbound_message(msg)
        publishers = self.dispatcher.transport_publisher
        # NOTE: transport_3 is the default transport
        self.assertEqual(publishers['transport_3'].msgs, [msg])

    @inlineCallbacks
    def test_dispatch_unresolveable_transport(self):
        # NOTE: we're override the value set by the configuration to
        #       stop the default fallback behaviour
        self.router.default_transport = None
        msg = self.msg_helper.make_outbound("1", to_addr='27123456789')
        f = yield self.assertFailure(
            self.router.dispatch_outbound_message(msg), DispatcherError)
        self.assertEqual(
            str(f), "Unable to dispatch outbound message for MNO None.")

    @inlineCallbacks
    def test_dispatch_unroutable_mno(self):
        # NOTE: we're override the value set by the configuration to
        #       stop the default fallback behaviour
        self.router.default_transport = None
        yield self.portia.annotate(
            '27123456789', key='observed-network', value='SURPRISE!',
            timestamp=self.portia.now())
        msg = self.msg_helper.make_outbound("1", to_addr='27123456789')
        f = yield self.assertFailure(
            self.router.dispatch_outbound_message(msg), DispatcherError)
        self.assertEqual(
            str(f), "Unable to dispatch outbound message for MNO SURPRISE!.")
