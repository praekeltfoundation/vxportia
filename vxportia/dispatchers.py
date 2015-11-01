from twisted.internet.defer import inlineCallbacks
from twisted.internet.protocol import Factory
from twisted.internet import reactor

from vumi.config import ConfigDict, ConfigClientEndpoint
from vumi.dispatchers.endpoint_dispatchers import Dispatcher
from vumi.errors import DispatcherError
from vumi.utils import normalize_msisdn

from vxportia.protocol import PortiaProtocol


def portia_normalize_msisdn(msisdn):
    # Portia expects MSISDNs without a leading +
    return normalize_msisdn(msisdn)[1:]


class PortiaDispatcherConfig(Dispatcher.CONFIG_CLASS):
    client_endpoint = ConfigClientEndpoint(
        'The Twisted Endpoint to use when connecting to the Portia server.',
        required=True, static=True)
    mapping = ConfigDict(
        "How transport names map endpoints and to MNOs. Format is: "
        "transport_name -> endpoint -> list of MNO names.",
        required=True, static=True)


class PortiaDispatcher(Dispatcher):

    CONFIG_CLASS = PortiaDispatcherConfig
    clock = reactor

    @inlineCallbacks
    def setup_dispatcher(self):
        config = self.get_static_config()

        self.reverse_mno_map = {}
        declared_mnos = []
        for transport, endpoints in config.mapping.items():
            for endpoint, mno in endpoints.items():
                declared_mnos.append(mno)
                self.reverse_mno_map[mno] = [transport, endpoint]

        if len(set(declared_mnos)) != len(declared_mnos):
            raise DispatcherError('PortiaDispatcher mappings are not unique.')

        if len(config.receive_outbound_connectors) != 1:
            raise DispatcherError(
                ('PortiaRouter is only able to work with 1 receive outbound '
                 'connector, there are %s configured.') % (
                    len(config.receive_outbound_connectors,)))

        self.ro_connector = config.receive_outbound_connectors[0]
        self.portia = yield config.client_endpoint.connect(
            Factory.forProtocol(PortiaProtocol))
        self.portia.clock = self.clock

    def teardown_dispatcher(self):
        # NOTE: the teardown still gets called even if we raised an exception
        #       during the setup_dispatcher so `portia` can be non existent.
        if hasattr(self, 'portia'):
            self.portia.transport.loseConnection()

    def process_inbound(self, config, msg, connector_name):
        endpoint_name = msg.get_routing_endpoint()
        endpoints = config.mapping.get(connector_name)
        if not endpoints:
            raise DispatcherError('No endpoints configured for %s.' % (
                connector_name,))

        mno = endpoints.get(endpoint_name)
        if not mno:
            raise DispatcherError('No MNO configured for %s:%s.' % (
                connector_name, endpoint_name))

        d = self.portia.annotate(
            portia_normalize_msisdn(msg['from_addr']),
            key='observed-network',
            value=mno)
        d.addCallback(
            lambda _: self.publish_inbound(msg, self.ro_connector, 'default'))
        return d

    def process_outbound(self, config, msg, connector_name):
        msisdn = portia_normalize_msisdn(msg['to_addr'])
        d = self.portia.resolve(msisdn)
        d.addCallback(
            lambda response: self.reverse_mno_map[response['network']])
        d.addCallback(
            lambda target: self.publish_outbound(msg, target[0], target[1]))
        return d

    def process_event(self, config, event, connector_name):
        return self.publish_event(event, self.ro_connector, 'default')