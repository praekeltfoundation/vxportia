from twisted.internet.defer import inlineCallbacks, returnValue, Deferred
from twisted.internet.protocol import Factory
from twisted.internet import reactor

from vumi.config import ConfigDict, ConfigClientEndpoint
from vumi.dispatchers.endpoint_dispatchers import Dispatcher
from vumi.errors import DispatcherError
from vumi.reconnecting_client import ReconnectingClientService
from vumi.utils import normalize_msisdn

from vxportia.protocol import PortiaProtocol


def portia_normalize_msisdn(msisdn):
    # Portia expects MSISDNs without a leading +
    return normalize_msisdn(msisdn)[1:]


class PortiaDispatcherConfig(Dispatcher.CONFIG_CLASS):
    portia_endpoint = ConfigClientEndpoint(
        'The Twisted Endpoint to use when connecting to the Portia server.',
        required=True, static=True)
    mapping = ConfigDict(
        "How transport names map endpoints and to MNOs. Format is: "
        "transport_name -> endpoint -> MNO name.",
        required=True, static=True)

    def post_validate(self):
        declared_mnos = []
        mapped_transports = []
        for transport, endpoints in self.mapping.items():
            for endpoint, mno in endpoints.items():
                declared_mnos.append(mno)
                mapped_transports.append(transport)

        if len(set(declared_mnos)) != len(declared_mnos):
            raise DispatcherError('PortiaDispatcher mappings are not unique.')

        if set(mapped_transports) != set(self.receive_inbound_connectors):
            raise DispatcherError(
                'Not all receive_inbound_connectors mapped to MNOs.')

        if len(self.receive_outbound_connectors) != 1:
            raise DispatcherError(
                ('PortiaRouter is only able to work with 1 receive outbound '
                 'connector, there are %s configured.') % (
                    len(self.receive_outbound_connectors,)))


class PortiaClientService(ReconnectingClientService):

    def __init__(self, dispatcher, endpoint, factory):
        self.dispatcher = dispatcher
        ReconnectingClientService.__init__(self, endpoint, factory)

    def clientConnected(self, protocol):
        ReconnectingClientService.clientConnected(self, protocol)
        self.dispatcher.clientConnected(protocol)

    def clientConnectionLost(self, reason):
        self.dispatcher.clientConnectionLost(reason)
        ReconnectingClientService.clientConnectionLost(self, reason)


class PortiaDispatcher(Dispatcher):

    CONFIG_CLASS = PortiaDispatcherConfig
    clock = reactor

    def setup_dispatcher(self):
        config = self.get_static_config()

        self.reverse_mno_map = {}
        for transport, endpoints in config.mapping.items():
            for endpoint, mno in endpoints.items():
                self.reverse_mno_map[mno] = [transport, endpoint]

        self.ro_connector = config.receive_outbound_connectors[0]

        self._portia = None
        self.portia_service = PortiaClientService(
            self, config.portia_endpoint, Factory.forProtocol(PortiaProtocol))
        self.portia_service.clock = self.clock
        self.portia_service.startService()

    def teardown_dispatcher(self):
        self.portia_service.stopService()

    def clientConnected(self, protocol):
        self._portia = protocol

    def clientConnectionLost(self, reason):
        self._portia = None

    def get_portia(self, timeout=10):
        d = Deferred()

        def force_timeout():
            if not d.called:
                d.errback(
                    DispatcherError(
                        'Unable to connect to Portia after %s seconds.' % (
                            timeout,)))

        assasin = self.clock.callLater(timeout, force_timeout)

        def cb():
            if self._portia:
                assasin.cancel()
                d.callback(self._portia)
                return
            self.clock.callLater(0.05, cb)

        cb()

        return d

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

        d = self.get_portia()
        d.addCallback(lambda portia: portia.annotate(
            portia_normalize_msisdn(msg['from_addr']),
            key='observed-network', value=mno))
        d.addCallback(
            lambda _: self.publish_inbound(msg, self.ro_connector, 'default'))
        return d

    @inlineCallbacks
    def process_outbound(self, config, msg, connector_name):
        msisdn = portia_normalize_msisdn(msg['to_addr'])
        portia = yield self.get_portia()
        response = yield portia.resolve(msisdn)
        if not response['network']:
            raise DispatcherError(
                ('Unable to route outbound message to: %s. '
                 'Portia was unable to resolve: %r.') % (
                    msisdn, response))
        target = self.reverse_mno_map.get(response['network'])
        if not target:
            raise DispatcherError(
                ('Unable to route outbound message to: %s. '
                 'No mapping for: %r.') % (
                    msg['to_addr'], response['network']))
        msg = yield self.publish_outbound(msg, target[0], target[1])
        returnValue(msg)

    def process_event(self, config, event, connector_name):
        return self.publish_event(event, self.ro_connector, 'default')
