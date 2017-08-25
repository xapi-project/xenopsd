import unittest
from contextlib import contextmanager

import time

import sys
from mock import patch, PropertyMock, MagicMock

# mock `xcp` module since this repo does not have it
sys.modules['xcp'] = MagicMock()

sys.path.append('./scripts')

from igmp_query_injector import Singleton, SubprocessAutoClean, XenstoreWatcher, IGMPQueryGenerator, VifsInjector, \
    BridgeInjector


class TestSingleton(unittest.TestCase):
    class _SingletonCls(object):
        __metaclass__ = Singleton

    def test_singleton(self):
        obj0 = self._SingletonCls()
        obj1 = self._SingletonCls()
        self.assertEqual(id(obj0), id(obj1))


class TestSubprocessAutoClean(unittest.TestCase):
    class _Subprocess(object):
        kill_cnt = [0]

        def terminate(self):
            self.kill_cnt[0] += 1

    def test_auto_clean(self):
        subprocess_cnt = 5
        with SubprocessAutoClean():
            for _ in range(subprocess_cnt):
                SubprocessAutoClean().add_subprocess(self._Subprocess())

        self.assertEqual(self._Subprocess.kill_cnt[0], subprocess_cnt)
        self.assertEqual(len(SubprocessAutoClean().subprocess_lst), 0)


class TestXenstoreWatcher(unittest.TestCase):
    @patch('subprocess.Popen')
    def test_start_terminate(self, MockPopen):
        watcher = XenstoreWatcher([])
        self.assertIsNone(watcher.p)
        self.assertEqual(len(SubprocessAutoClean().subprocess_lst), 0)
        watcher.start()
        self.assertEqual(len(SubprocessAutoClean().subprocess_lst), 1)
        watcher.terminate()
        self.assertEqual(len(SubprocessAutoClean().subprocess_lst), 0)
        watcher.p.terminate.assert_called_once()


class TestIGMPQueryGenerator(unittest.TestCase):
    def test_create_igmp_layer(self):
        gen = IGMPQueryGenerator('00:00:00:00:00:00', -1, 10000)
        layer = gen.create_igmp_layer()
        self.assertListEqual(layer, [0x11, 0x64, 0xee, 0x9b, 0x00, 0x00, 0x00, 0x00])


class TestVifsInjector(unittest.TestCase):
    @contextmanager
    def assert_time_elapse_in(self, min, max):
        start = time.time()
        yield
        elapse = time.time() - start
        self.assertTrue(min <= elapse <= max)

    @patch('igmp_query_injector.Vif')
    @patch('igmp_query_injector.IGMPQueryInjector.inject_to_vif')
    @patch('igmp_query_injector.VifsInjector._inject_with_connection_state_check')
    def test_inject_without_connection_state_check(self, mock_inject_with_connection_state_check,
                                                   mock_inject_to_vif, MockVif):
        injector = VifsInjector(100, ['vif1.1', 'vif2.1'], 0)
        injector.inject()
        mock_inject_with_connection_state_check.assert_not_called()
        self.assertEqual(mock_inject_to_vif.call_count, 2)

    @patch('igmp_query_injector.XenstoreWatcher.stdout', new_callable=PropertyMock)
    @patch('subprocess.Popen')
    @patch('igmp_query_injector.IGMPQueryInjector.inject_to_vif')
    @patch('igmp_query_injector.XenstoreUtil.read')
    def test_inject_with_connection_state_check(self, mock_xenstore_read, mock_inject_to_vif, MockPopen,
                                                mock_xenstore_watcher_stdout):
        # set it to stdin
        mock_xenstore_watcher_stdout.return_value = 0
        mock_xenstore_read.return_value = ''
        injector = VifsInjector(100, ['vif1.1', 'vif2.1'], 3)
        # epoll should timeout in 3 seconds
        with self.assert_time_elapse_in(2, 4):
            injector.inject()
        # won't inject query to any vif
        mock_inject_to_vif.assert_not_called()
        self.assertEqual(MockPopen.call_count, 1)
        self.assertEqual(len(SubprocessAutoClean().subprocess_lst), 0)


class TestBridgeInjector(unittest.TestCase):
    @patch('subprocess.check_output')
    def test_get_vifs_on_bridge(self, mock_subprocess_check_output):
        mock_subprocess_check_output.return_value = 'vif1.1\ntap1.0\nvif1.2'
        injector = BridgeInjector(100, ['xenbr0'])
        vifs = injector.get_vifs_on_bridge('xenbr0')
        self.assertEqual(len(vifs), 2)


if __name__ == '__main__':
    unittest.main()
