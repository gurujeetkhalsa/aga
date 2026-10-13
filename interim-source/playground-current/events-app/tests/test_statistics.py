import unittest
from unittest.mock import Mock,patch
import azure.functions as func
from function_app import workshop_registration_statistics

class StatisticsHttpTests(unittest.TestCase):
    def request(self,params=None):
        return func.HttpRequest(method='GET',url='https://workshop.test/api/events/manage/1/statistics',
            route_params={'event_id':'1'},params=params or {},body=b'')

    def test_statistics_require_organizer_signin(self):
        with patch('function_app._repository') as repo:
            self.assertEqual(workshop_registration_statistics(self.request()).status_code,401)
            repo.assert_not_called()

    def test_date_filter_validation_and_authorized_request(self):
        repo=Mock();repo.registration_statistics.return_value={'totals':{},'chapters':[]}
        with patch('function_app._organizer_principal',return_value='admin'),patch('function_app._repository',return_value=repo):
            for value in ['-1','0','abc','1 OR 1=1','9'*25,'١']:
                self.assertEqual(workshop_registration_statistics(self.request({'occurrence_id':value})).status_code,400)
            repo.registration_statistics.assert_not_called()
            response=workshop_registration_statistics(self.request({'occurrence_id':'23'}))
            self.assertEqual(response.status_code,200)
            self.assertEqual(response.headers['Cache-Control'],'no-store')
            self.assertEqual(repo.registration_statistics.call_args.kwargs['occurrence_id'],23)
            self.assertEqual(repo.registration_statistics.call_args.kwargs['principal_id'],'admin')
