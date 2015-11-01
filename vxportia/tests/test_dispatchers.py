import pkg_resources

from portia.portia import Portia
from portia.utils import (
    start_redis, start_tcpserver, compile_network_prefix_mappings)

from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import Clock

from vumi.dispatchers.tests.helpers import DispatcherHelper
from vumi.errors import DispatcherError
from vumi.tests.helpers import VumiTestCase

from vxportia.dispatchers import PortiaDispatcher, portia_normalize_msisdn


class TestPortiaDispatcher(VumiTestCase):

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

        # NOTE: setting the clock before the setup_dispatcher stuff is called
        PortiaDispatcher.clock = Clock()
        self.disp_helper = self.add_helper(DispatcherHelper(PortiaDispatcher))

    def get_dispatcher(self, **config_extras):
        config = {
            "receive_inbound_connectors": ["transport1", "transport2"],
            "receive_outbound_connectors": ["app1"],
            "portia_endpoint": "tcp:%s:%s" % (self.listener_host,
                                              self.listener_port),
            "mapping": {
                "transport1": {
                    "default": "mno1"
                },
                "transport2": {
                    "default": "mno2"
                },
            }
        }
        config.update(config_extras)
        return self.disp_helper.get_dispatcher(config)

    def ch(self, connector_name):
        return self.disp_helper.get_connector_helper(connector_name)

    def assert_rkeys_used(self, *rkeys):
        broker = self.disp_helper.worker_helper.broker
        self.assertEqual(set(rkeys), set(broker.dispatched['vumi'].keys()))

    def assert_dispatched_endpoint(self, msg, endpoint, dispatched_msgs):
        msg.set_routing_endpoint(endpoint)
        self.assertEqual([msg], dispatched_msgs)

    def test_unique_mno_config(self):
        failure = self.assertRaises(
            DispatcherError,
            self.get_dispatcher,
            mapping={
                'transport1': {
                    "default": 'mno1',
                },
                'transport2': {
                    "default": 'mno1',
                    "ep1": 'mno2',
                },
            })
        self.assertEqual(
            str(failure),
            'PortiaDispatcher mappings are not unique.')

    def test_single_receive_outbound_connector(self):
        failure = self.assertRaises(
            DispatcherError, self.get_dispatcher,
            receive_outbound_connectors=['app1', 'app2'])
        self.assertEqual(
            str(failure),
            ('PortiaRouter is only able to work with 1 receive outbound '
             'connector, there are 2 configured.'))

    @inlineCallbacks
    def test_inbound_message_routing(self):
        from_addr = '+27123456789'
        resolve_response = yield self.portia.resolve(
            portia_normalize_msisdn(from_addr))
        self.assertEqual(resolve_response['network'], None)
        yield self.get_dispatcher()
        msg = yield self.ch("transport1").make_dispatch_inbound(
            "inbound", from_addr=from_addr)
        self.assert_rkeys_used('transport1.inbound', 'app1.inbound')
        self.assert_dispatched_endpoint(
            msg, 'default', self.ch('app1').get_dispatched_inbound())
        resolve_response = yield self.portia.resolve(
            portia_normalize_msisdn(from_addr))
        self.assertEqual(resolve_response['network'], 'mno1')

    @inlineCallbacks
    def test_inbound_event_routing(self):
        yield self.get_dispatcher()
        msg = yield self.ch('transport1').make_dispatch_ack()
        self.assert_rkeys_used('transport1.event', 'app1.event')
        self.assert_dispatched_endpoint(
            msg, 'default', self.ch('app1').get_dispatched_events())

    @inlineCallbacks
    def test_outbound_message_routing(self):
        to_addr = '+27123456789'
        yield self.portia.annotate(
            portia_normalize_msisdn(to_addr),
            key='observed-network', value='mno1',
            timestamp=self.portia.now())
        yield self.get_dispatcher()
        msg = yield self.ch('app1').make_dispatch_outbound(
            "outbound", to_addr=to_addr)
        self.assert_rkeys_used('app1.outbound', 'transport1.outbound')
        self.assert_dispatched_endpoint(
            msg, 'default', self.ch('transport1').get_dispatched_outbound())
