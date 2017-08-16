#!/usr/bin/env python
import argparse
import os
import socket
import subprocess
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from select import EPOLLIN, epoll
import logging
import signal
import resource
import xcp.logger as log
import time
import sys
import re

__DEBUG = False
if __DEBUG:
    logging_lvl = logging.DEBUG
else:
    logging_lvl = logging.INFO


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class SubprocessAutoClean(object):
    __metaclass__ = Singleton
    """Ensure all subprocess are terminated when the main process exits
    """
    CATCH_SIGNALS = [signal.SIGTERM, signal.SIGINT]

    def __init__(self):
        self.subprocess_lst = []

    def add_subprocess(self, p):
        self.subprocess_lst.append(p)

    def del_subprocess(self, p):
        self.subprocess_lst.remove(p)

    def kill_all_subprocess(self):
        if not self.subprocess_lst:
            return

        log.info('kill all sub process')
        for p in self.subprocess_lst:
            p.terminate()

        self.subprocess_lst = []

    def __enter__(self):
        self._set_signal_handler(self.CATCH_SIGNALS, self._kill_all_subprocess_and_exit)

    def __exit__(self, exc_type, exc_value, traceback):
        self.kill_all_subprocess()

    def _set_signal_handler(self, signals, f):
        for s in signals:
            signal.signal(s, f)

    def _kill_all_subprocess_and_exit(self, signum, frame):
        log.critical('Catch signal %d, kill all subprocess' % signum)
        self.kill_all_subprocess()
        sys.exit(ERRNO.SIGNAL_CATCH)


class ERRNO(object):
    SUCC = 0
    SIGNAL_CATCH = 1


class XenstoreUtil(object):
    """Utilities for access xenstore
    """
    @staticmethod
    def read(path):
        return subprocess.check_output(['xenstore-read', path]).strip()

    @staticmethod
    def vif_mac_path(domid, vifid):
        return '/local/domain/%d/device/vif/%d/mac' % (domid, vifid)

    @staticmethod
    def vif_state_path(domid, vifid):
        return '/local/domain/%d/device/vif/%d/state' % (domid, vifid)


class XenstoreWatcher(object):
    """Tool for watching xenstore
    """
    def __init__(self, watch_path_list):
        self._watch_path_list = watch_path_list
        self.p = None

    def start(self):
        self.p = subprocess.Popen(['xenstore-watch'] + self._watch_path_list, stdout=subprocess.PIPE)
        SubprocessAutoClean().add_subprocess(self.p)

    def terminate(self):
        self.p.terminate()
        SubprocessAutoClean().del_subprocess(self.p)

    def readline(self):
        return self.p.stdout.readline().strip()

    @property
    def stdout(self):
        return self.p.stdout


class Vif(object):
    def __init__(self, vif):
        self.vif_name = vif
        ids = self.vif_name.split('vif')[1].split('.')
        self._domid = int(ids[0])
        self._vifid = int(ids[1])
        self.mac = XenstoreUtil.read(self._mac_address_path())
        self.state_path = self._vif_state_path()

    def _mac_address_path(self):
        return XenstoreUtil.vif_mac_path(self._domid, self._vifid)

    def _vif_state_path(self):
        return XenstoreUtil.vif_state_path(self._domid, self._vifid)


class VifStateChecker(object):
    def __init__(self, vif, expect_state):
        self._vif = Vif(vif)
        self.expect_state = expect_state

    @property
    def vif(self):
        return self._vif.vif_name

    @property
    def mac(self):
        return self._vif.mac

    @property
    def vif_state_path(self):
        return self._vif.state_path

    def state_is_expected(self):
        return XenstoreUtil.read(self.vif_state_path) == self.expect_state


class IGMPQueryGenerator(object):
    """IGMP Query generator.
    Generate IGMP Query packet with/without vlan
    """
    def __init__(self, dst_mac, vlanid, max_resp_time):
        """
        :param dst_mac: Destination mac address of this IGMP query packet
        :param vlanid: Vlan ID of this packet. `-1` means no vlan
        :param max_resp_time: Max response time of IGMP query. Unit is 100ms
        """
        self.src_mac = '00:00:00:00:00:00'
        self.dst_mac = dst_mac
        self.vlanid = vlanid
        self.max_resp_time = max_resp_time

    def parse_mac_address(self, mac):
        ret = []
        for x in mac.split(':'):
            ret.append(int(x, 16))

        return ret

    def create_ether_layer(self):
        ret = []
        for mac in (self.dst_mac, self.src_mac):
            ret.extend(self.parse_mac_address(mac))

        if self.vlanid == -1:
            type_field = [0x08, 0x00]
        else:
            type_field = [0x81, 0x00]

        ret.extend(type_field)
        return ret

    def create_vlan_layer(self):
        if self.vlanid == -1:
            return []
        else:
            return [0x20 | (self.vlanid >> 8), self.vlanid & 0xff, 0x08, 0x00]

    def create_ip_layer(self):
        """
        IP Type: IPv4 (0x0800)
        Version: 4
        Total length: 32
        TTL: 1
        Source IP: 0.0.0.0
        Destination IP: 224.0.0.1 (e0:00:00:01)
        """
        return [
            0x46, 0x00, 0x00, 0x20, 0x00, 0x01,
            0x00, 0x00, 0x01, 0x02, 0x44, 0xd6,
            0x00, 0x00, 0x00, 0x00, 0xe0, 0x00,
            0x00, 0x01, 0x94, 0x04, 0x00, 0x00,
        ]

    def create_igmp_layer(self):
        """
        IGMP Version: 2
        IGMP Type: IGMP Query (0x11)
        Max Resp Time: self.max_resp_time
        Multicast Address: 0.0.0.0
        """
        msg_type = 0x11
        # MAX_RESP_TIME filed unit is 100ms
        max_resp_time = self.max_resp_time / 100
        group_address = [0x00, 0x00, 0x00, 0x00]
        chksum = self._calc_igmp_chksum(msg_type, max_resp_time, group_address)
        ret = [msg_type, max_resp_time] + chksum + group_address
        return ret

    def _calc_igmp_chksum(self, msg_type, max_resp_time, group_address):
        msg = [msg_type, max_resp_time, 0x00, 0x00] + group_address
        chksum = socket.htons(self._checksum(msg))
        return [(chksum & 0xff00) >> 8, chksum & 0xff]

    def _checksum(self, msg):
        def carry_around_add(a, b):
            c = a + b
            return (c & 0xffff) + (c >> 16)

        s = 0
        for i in range(0, len(msg), 2):
            w = msg[i] + (msg[i+1] << 8)
            s = carry_around_add(s, w)
        return ~s & 0xffff

    def generate(self):
        ether_layer = self.create_ether_layer()
        vlan_layer = self.create_vlan_layer()
        ip_layer = self.create_ip_layer()
        igmp_layer = self.create_igmp_layer()
        packet = ether_layer + vlan_layer + ip_layer + igmp_layer
        return "".join(map(chr, packet))


class IGMPQueryInjector(object):
    __metaclass__ = ABCMeta

    def __init__(self, max_resp_time):
        self.max_resp_time = max_resp_time

    def inject_to_vif(self, vif, mac):
        log.info('Inject IGMP query to vif:%s, mac:%s' % (vif, mac))
        packet = IGMPQueryGenerator(mac, -1, self.max_resp_time).generate()
        self.inject_query_packet(vif, packet)

    def inject_to_vifs(self, vifs):
        for vif in vifs:
            _vif = Vif(vif)
            self.inject_to_vif(_vif.vif_name, _vif.mac)

    def inject_query_packet(self, interface, packet):
        s = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, 0)
        s.bind((interface, 0))
        s.send(packet)
        s.close()

    @abstractmethod
    def inject(self):
        pass


class VifsInjector(IGMPQueryInjector):
    def __init__(self, max_resp_time, vifs, vif_connected_timeout=0):
        super(VifsInjector, self).__init__(max_resp_time)
        self.vifs = vifs
        self.vif_connected_timeout = vif_connected_timeout

    def inject(self):
        if self.vif_connected_timeout > 0:
            # should check connection state
            log.info('Inject IGMP query with connection state check')
            self._inject_with_connection_state_check()
        else:
            log.info('Inject IGMP query without connection state check')
            self._inject_without_connection_state_check()

    def _inject_with_connection_state_check(self):
        checkers = self._build_vif_state_checkers()
        with self._start_watcher(checkers) as (watcher, poll):
            remain_timeout = self.vif_connected_timeout
            while remain_timeout > 0:
                start = time.time()
                events = poll.poll(remain_timeout)
                if not events:
                    break

                for _ in events:
                    path = watcher.readline()
                    if path not in checkers:
                        continue

                    checker = checkers[path]
                    if checker.state_is_expected():
                        self.inject_to_vif(checker.vif, checker.mac)
                        del checkers[path]

                if not checkers:
                    # all of the vif state has changed to connected
                    break

                remain_timeout -= (time.time() - start)

        for checker in checkers.itervalues():
            log.warning("Value of '%s' not change to '%s' in %d seconds." %
                        (checker.vif_state_path, checker.expect_state, self.vif_connected_timeout))
            log.warning("Won't inject IGMP query to vif: %s, mac: %s" % (checker.vif, checker.mac))

    def _build_vif_state_checkers(self):
        vif_state_connected = '4'
        checkers = [VifStateChecker(vif, vif_state_connected) for vif in self.vifs]
        return dict([(checker.vif_state_path, checker) for checker in checkers])

    @contextmanager
    def _start_watcher(self, checkers):
        watcher = XenstoreWatcher(checkers.keys())
        watcher.start()
        poll = epoll()
        poll.register(watcher.stdout, EPOLLIN)
        yield watcher, poll

        poll.close()
        watcher.terminate()

    def _inject_without_connection_state_check(self):
        self.inject_to_vifs(self.vifs)


class BridgeInjector(IGMPQueryInjector):
    RE_VIF = re.compile(r'^vif\d+\.\d+$')

    def __init__(self, max_resp_time, bridges):
        super(BridgeInjector, self).__init__(max_resp_time)
        self.bridges = bridges

    def get_vifs_on_bridge(self, bridge):
        ret = []
        outs = subprocess.check_output(['ovs-vsctl', 'list-ports', bridge]).strip()
        for line in outs.split('\n'):
            if self.RE_VIF.match(line):
                ret.append(line)

        return ret

    def inject(self):
        for bridge in self.bridges:
            log.info('Inject IGMP query to bridge:%s' % bridge)
            vifs = self.get_vifs_on_bridge(bridge)
            self.inject_to_vifs(vifs)


def inject_to_vifs(args):
    log.debug('Entry point: Inject IGMP query per pif')
    injector = VifsInjector(args.max_resp_time, args.vifs, args.vif_connected_timeout)
    return injector.inject()


def inject_to_vifs_on_bridges(args):
    log.debug('Entry point: Inject IGMP query per bridge')
    injector = BridgeInjector(args.max_resp_time, args.bridges)
    return injector.inject()


def build_parser():
    parser = argparse.ArgumentParser(prog='igmp_query_injector.py', description=
                                     'Tool for injecting IGMP query packet')
    parser.add_argument('--detach', dest='detach', required=False, action='store_true',
                        help='execute this tool as a daemon')
    parser.add_argument('--max-resp-time', dest='max_resp_time', required=False, metavar='max_resp_time', type=int,
                        default=100, help='max response time of IGMP query, unit is millisecond')

    subparsers = parser.add_subparsers()
    to_vif_parser = subparsers.add_parser('vif', help='inject query to vifs',
                                          description='Inject query to vifs')
    to_vif_parser.set_defaults(func=inject_to_vifs)
    to_vif_parser.add_argument('vifs', metavar='vif_name', nargs='+', help='vif interface name in Dom0')
    to_vif_parser.add_argument('--wait-vif-connected', dest='vif_connected_timeout', metavar='timeout', type=int,
                               default=0, help='timeout value for waiting vif connected, unit is second')

    to_bridge_parser = subparsers.add_parser('bridge', help='inject query to vifs on the bridge',
                                             description='Inject query to vifs on the bridge')
    to_bridge_parser.set_defaults(func=inject_to_vifs_on_bridges)
    to_bridge_parser.add_argument('bridges', metavar='bridge_name', nargs='+', help='bridge name of OVS')

    return parser


def _detach():
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
        else:
            maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
            if maxfd == resource.RLIM_INFINITY:
                maxfd = 1024

            # Iterate through and close all file descriptors.
            for fd in range(0, maxfd):
                try:
                    os.close(fd)
                except OSError:
                    pass
    except OSError as e:
        sys.stderr.write("fork failed: %d (%s)\n" % (e.errno, e.strerror))
        sys.exit(1)


def toggle_of_IGMP_snooping_is_enabled():
    pool_uuid = subprocess.check_output(['/opt/xensource/bin/xe', 'pool-list', '--minimal']).strip()
    toggle_state = subprocess.check_output(
        ['/opt/xensource/bin/xe', 'pool-param-get', 'uuid=%s' % pool_uuid, 'param-name=igmp-snooping-enabled']).strip()
    return toggle_state == 'true'


def network_backend_is_openvswitch():
    bridge_type = subprocess.check_output(['/opt/xensource/bin/xe-get-network-backend']).strip()
    return bridge_type == 'openvswitch'


def main():
    log.logToSyslog(level=logging_lvl)
    parser = build_parser()
    args = parser.parse_args()

    if args.detach:
        _detach()

    if not toggle_of_IGMP_snooping_is_enabled():
        log.info('IGMP snooping is off, no need to inject query')
        return ERRNO.SUCC

    if not network_backend_is_openvswitch():
        log.info('Network backend type is not openvswitch, no need to inject query')
        return ERRNO.SUCC

    # Ensure kill subprocess when exception/signal catches
    with SubprocessAutoClean():
        args.func(args)


if __name__ == '__main__':
    main()
