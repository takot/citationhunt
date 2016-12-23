import mock
import chdb

import unittest

class ConnectionPoolTest(unittest.TestCase):
    def setUp(self):
        self.connection_pool = chdb.ConnectionPool()
        self.mock_conn1 = mock.Mock()
        self.mock_conn2 = mock.Mock()
        chdb.MySQLdb.connect = mock.Mock(wraps = self._mock_connect)

    def _mock_connect(self, *args, **kwds):
        if kwds['read_default_file'] == 'config1':
            return self.mock_conn1
        return self.mock_conn2

    def test_get_connection_one_per_config(self):
        mock_initializer = mock.Mock()
        self.assertIs(self.mock_conn1,
            self.connection_pool.get_connection('config1', mock_initializer))
        self.assertEquals(chdb.MySQLdb.connect.call_count, 1)
        self.assertEquals(
            'config1', chdb.MySQLdb.connect.call_args[1]['read_default_file'])
        self.assertIs(
            chdb.RetryingCursor,
            chdb.MySQLdb.connect.call_args[1]['cursorclass'])
        mock_initializer.assert_called_once_with(self.mock_conn1)

        # Request a connection with a different config, which should cause
        # chdb.MySQLdb.connect() to be called again.
        self.connection_pool.get_connection('config2', mock_initializer)
        self.assertEquals(
            'config2', chdb.MySQLdb.connect.call_args[1]['read_default_file'])

    def test_get_connection_returned(self):
        mock_initializer = mock.Mock()
        self.assertIs(self.mock_conn1,
            self.connection_pool.get_connection('config1', mock_initializer))
        self.connection_pool.return_connection(self.mock_conn1)
        self.assertEquals(mock_initializer.call_count, 1)
        # Should get the same connection again, without another call to
        # connect(), but calling the initializer again
        self.assertEquals(chdb.MySQLdb.connect.call_count, 1)
        self.assertIs(self.mock_conn1,
            self.connection_pool.get_connection('config1', mock_initializer))
        self.assertEquals(mock_initializer.call_count, 2)

    def test_return_on_close(self):
        self.assertIs(self.mock_conn1,
            self.connection_pool.get_connection('config1', None))
        self.mock_conn1.close()
        self.assertIs(self.mock_conn1,
            self.connection_pool.get_connection('config1', None))

    def test_get_connection_same_config_twice(self):
        self.connection_pool.get_connection('config1', None)
        self.connection_pool.get_connection('config1', None)
        self.assertEquals(chdb.MySQLdb.connect.call_count, 2)

        # now return the connection, shouldn't connect again
        self.connection_pool.return_connection(self.mock_conn1)
        self.connection_pool.get_connection('config1', None)
        self.assertEquals(chdb.MySQLdb.connect.call_count, 2)

    def test_swap_connection(self):
        mock_initializer = mock.Mock()
        self.assertIs(self.mock_conn1,
            self.connection_pool.get_connection('config1', mock_initializer))
        self.assertEquals(chdb.MySQLdb.connect.call_count, 1)
        self.assertEquals(mock_initializer.call_count, 1)

        # we don't actually return the same connection, this is an artifact
        # of the test. really_close is also an implementation detail, but it's
        # easier to use it in testing.
        self.mock_conn1.closed = 0
        self.mock_conn1.really_close = mock.Mock()
        self.connection_pool.return_connection = mock.Mock()
        self.assertIs(self.mock_conn1,
            self.connection_pool.swap_connection(self.mock_conn1))

        # must have not have closed the connection, and not returned it
        self.mock_conn1.really_close.assert_not_called()
        self.connection_pool.return_connection.assert_not_called()

        self.assertEquals(mock_initializer.call_count, 2)
        self.assertEquals(chdb.MySQLdb.connect.call_count, 2)

class RetryingCursorTest(unittest.TestCase):
    def test_refresh_connection_on_exception(self):
        self.count = 0
        def raise_three_times(*args, **kwds):
            if self.count == 3:
                return
            self.count += 1
            raise chdb.MySQLdb.OperationalError('gone away')

        mock_conn = mock.Mock()
        mock_conn.next_result = mock.Mock(return_value = -1)
        chdb.MySQLdb.cursors.Cursor.execute = mock.Mock(
            wraps = raise_three_times)
        chdb.MySQLdb.connect = mock.Mock(return_value = mock_conn)
        chdb._connection_pool.swap_connection = mock.Mock(
            wraps = chdb._connection_pool.swap_connection)

        chdb._connection_pool.get_connection('config1')  # initialize mock_conn
        cursor = chdb.RetryingCursor(mock_conn)
        cursor.execute('SELECT 1')

        self.assertEquals(chdb._connection_pool.swap_connection.call_count, 3)

if __name__ == '__main__':
    unittest.main()
