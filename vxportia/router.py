
from vumi.errors import DispatcherError
from vumi.dispatchers.base import BaseDispatchRouter

from twisted.internet import reactor
from twisted.internet.protocol import Factory
from twisted.internet.endpoints import clientFromString
from twisted.internet.defer import inlineCallbacks

from vxportia.protocol import PortiaProtocol


class PortiaRouter(BaseDispatchRouter):
    """
    A router that connects to Portia, the number porting database,
    to map incoming messages to MNOs and to determine how outbound
    messages are meant to be routed back.
    """

    clock = reactor

    @inlineCallbacks
    def setup_routing(self):
        self.portia_config = self.config['portia']

        self.mno_transport_map = self.portia_config['mappings']
        self.transport_mno_map = dict([
            (transport_name, mno)
            for mno, transport_name in self.mno_transport_map.items()])
        self.default_transport = self.portia_config.get('default_transport')

        known_transport_names = self.mno_transport_map.values()
        if self.default_transport:
            known_transport_names.append(self.default_transport)

        if (set(known_transport_names) !=
                set(self.config['transport_names'])):
            raise DispatcherError(
                'PortiaRouter mappings differ from dispatcher '
                'transport_names.')

        if len(self.config['exposed_names']) != 1:
            raise DispatcherError(
                ('PortiaRouter is only able to work with 1 exposed_name, '
                 'there are %s configured.') % (
                    len(self.config['exposed_names'],)))

        self.exposed_name = self.config['exposed_names'][0]

        self.endpoint = clientFromString(
            self.clock, self.portia_config['client_endpoint'])
        self.portia = yield self.endpoint.connect(
            Factory.forProtocol(PortiaProtocol))

    def teardown_routing(self):
        self.portia.transport.loseConnection()

    def dispatch_inbound_message(self, msg):
        return self.dispatch_inbound_message_from_mno(
            self.transport_mno_map[msg['transport_name']], msg.copy())

    def dispatch_inbound_event(self, event):
        return self.dispatch_inbound_event_from_mno(
            self.transport_mno_map[event['transport_name']], event.copy())

    def dispatch_outbound_message(self, msg):
        d = self.portia.resolve(msg['to_addr'])
        d.addCallback(self.dispatch_resolved_outbound_message, msg)
        return d

    def dispatch_inbound_message_from_mno(self, mno, msg):
        d = self.portia.annotate(
            msg['from_addr'], key='observed-network', value=mno)
        d.addCallback(
            lambda _: self.dispatcher.publish_inbound_message(
                self.exposed_name, msg.copy()))
        return d

    def dispatch_inbound_event_from_mno(self, mno, event):
        return self.dispatcher.publish_inbound_event(
            self.exposed_name, event.copy())

    def dispatch_resolved_outbound_message(self, portia_response, msg):
        transport_name = (
            self.mno_transport_map.get(portia_response['network'])
            or self.default_transport)
        print 'transport_name', transport_name
        if not transport_name:
            raise DispatcherError(
                'Unable to dispatch outbound message for MNO %s.' % (
                    portia_response['network'],))
        return self.dispatcher.publish_outbound_message(
            transport_name, msg.copy())
