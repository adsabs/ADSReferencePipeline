import sys, os
project_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
if project_home not in sys.path:
    sys.path.insert(0, project_home)

import unittest
from unittest.mock import MagicMock, patch
import json
import time
import requests

from adsrefpipe.utils import get_bibcode, verify_bibcode, post_request_resolved_reference, \
    get_date_created, get_date_modified_struct_time


class TestUtils(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)

    def tearDown(self):
        unittest.TestCase.tearDown(self)

    def test_get_bibcode(self):
        """ some reference files provide doi, and bibcode needs to be infered from doi """
        return_value = {
            u'responseHeader': {u'status': 0, u'QTime': 13},
            u'response': {
                u'start': 0,
                u'numFound': 1,
                u'docs': [{u'bibcode': u'2023arXiv230317899C'}]
            }
        }
        with patch('requests.get') as get_mock:
            get_mock.return_value = mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = json.dumps(return_value)
            bibcode = get_bibcode(doi='10.48550/arXiv.2303.17899')
            self.assertEqual(bibcode, '2023arXiv230317899C')

    def test_get_bibcode_error(self):
        """ some reference files provide doi, and bibcode needs to be infered from doi when solr returns error"""
        with patch('requests.get') as get_mock:
            get_mock.return_value = mock_response = MagicMock()
            mock_response.status_code = 502
            bibcode = get_bibcode(doi='10.48550/arXiv.2303.17899')
            self.assertEqual(bibcode, None)

    def test_get_bibcode_exception(self):
        """ test get_bibcode when a request exception occurs """
        with patch('requests.get') as get_mock:
            get_mock.side_effect = requests.exceptions.RequestException("Connection error")
            self.assertEqual(get_bibcode(doi='10.48550/arXiv.2303.17899'), None)

    def test_verify_bibcode(self):
        """ test calling solr to verify a bibcode """
        return_value = {
            u'responseHeader': {u'status': 0, u'QTime': 13},
            u'response': {
                u'start': 0,
                u'numFound': 1,
                u'docs': [{u'bibcode': u'2023arXiv230317899C'}]
            }
        }
        with patch('requests.get') as get_mock:
            get_mock.return_value = mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = json.dumps(return_value)
            bibcode = verify_bibcode(bibcode='2023arXiv230317899C')
            self.assertEqual(bibcode, '2023arXiv230317899C')

    def test_verify_bibcode_error(self):
        """ test calling solr to verify a bibcode when error is returned """
        with patch('requests.get') as get_mock:
            get_mock.return_value = mock_response = MagicMock()
            mock_response.status_code = 502
            bibcode = verify_bibcode(bibcode='2023arXiv230317899C')
            self.assertEqual(bibcode, '')

    def test_verify_bibcode_exception(self):
        """ test verify_bibcode when a request exception occurs """
        with patch('requests.get') as get_mock:
            get_mock.side_effect = requests.exceptions.RequestException("Connection error")
            self.assertEqual(verify_bibcode(bibcode='2023arXiv230317899C'), "")

    def test_get_resolved_references_error(self):
        """ test calling post_request_resolved_reference with wrong end point """
        references = [{'item_num': 2,
                       'refstr': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ',
                       'refraw': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   '}]
        self.assertEqual(post_request_resolved_reference(references, 'wrong_url'), None)

        with patch('requests.post') as get_mock:
            get_mock.return_value = mock_response = MagicMock()
            mock_response.status_code = 502
            self.assertEqual(post_request_resolved_reference(references, 'xml'), None)

    def test_post_request_resolved_reference_exception(self):
        """ test post_request_resolved_reference when a request exception occurs """
        references = [{'item_num': 2,
                       'refstr': 'Arcangeli, J., Desert, J.-M., Parmentier, V., et al. 2019, A&A, 625, A136   ',
                       'id': '2'}]

        with patch('requests.post') as get_mock:
            get_mock.side_effect = requests.exceptions.RequestException("Connection error")
            self.assertEqual(post_request_resolved_reference(references[0], 'text'), None)

    @patch("adsrefpipe.utils.path.getctime")
    @patch("adsrefpipe.utils.time.localtime")
    def test_get_date_created(self, mock_localtime, mock_getctime):
        """ test get_date_created method """

        # mock file creation time (epoch timestamp) corresponding to 2023-01-01 00:00:00 UTC
        mock_getctime.return_value = 1672531200

        # create struct_time object that matches what the function expects
        time_tuple = time.struct_time((2023, 1, 1, 0, 0, 0, 6, 1, 0))
        mock_localtime.return_value = time_tuple

        # patch DATE_FORMAT to match
        with patch("adsrefpipe.utils.DATE_FORMAT", "%04d/%02d/%02d %02d:%02d:%02d"):
            self.assertEqual(get_date_created("dummy_file.txt"), "2023/01/01 00:00:00")

    @patch("adsrefpipe.utils.path.getmtime")
    @patch("adsrefpipe.utils.time.localtime")
    def test_get_date_modified_struct_time(self, mock_localtime, mock_getmtime):
        """ test get_date_modified_struct_time method """

        # mock file modification time (epoch timestamp) corresponding to 2023-01-01 00:00:00 UTC
        mock_getmtime.return_value = 1672531200

        # create a struct_time object
        expected_time = time.struct_time((2023, 1, 1, 0, 0, 0, 6, 1, 0))
        mock_localtime.return_value = expected_time

        result = get_date_modified_struct_time("test_file.txt")

        # verify the mocks were called with correct arguments
        mock_getmtime.assert_called_once_with("test_file.txt")
        mock_localtime.assert_called_once_with(1672531200)

        self.assertEqual(result, expected_time)


if __name__ == '__main__':
    unittest.main()
