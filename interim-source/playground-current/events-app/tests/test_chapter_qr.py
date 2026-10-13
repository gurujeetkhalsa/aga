import json
import os
import unittest
from io import BytesIO
from unittest.mock import Mock, patch
from pathlib import Path
import azure.functions as func
from PIL import Image
import zxingcpp
from function_app import workshop_qr_card, workshop_qr_download, workshop_referral_chapters, events_register_free
from sql_repository import SqlEventRepository

BASE='https://aga-events.azurewebsites.net/events/learn-go-in-an-hour'
CHAPTER=dict(chapter_id=22385,chapter_code='NGOC',chapter_name='National Go Center')

class ChapterQrTests(unittest.TestCase):
    def request(self, fmt=None, ref='22385'):
        return func.HttpRequest(method='GET',url=BASE+'/workshop-qr'+('/'+fmt if fmt else ''),
            params={'ref':ref},route_params=dict(slug='learn-go-in-an-hour',**({'format':fmt} if fmt else {})),body=b'')

    def test_chapter_png_decodes_after_print_and_phone_resizing(self):
        repo=Mock();repo.get_referral_chapter.return_value=CHAPTER
        with patch('function_app._repository',return_value=repo),patch.dict(os.environ,{'EVENTS_PUBLIC_BASE_URL':BASE.split('/events/')[0]}):
            response=workshop_qr_download(self.request('png'))
        self.assertEqual(response.status_code,200)
        self.assertIn('chapter-22385',response.headers['Content-Disposition'])
        original=Image.open(BytesIO(response.get_body()))
        self.assertEqual(original.size,(1200,1200))
        for size in (1200,600,360,300,280):
            decoded=zxingcpp.read_barcode(original.resize((size,size)))
            self.assertIsNotNone(decoded,f'Cannot scan {size}px')
            self.assertEqual(decoded.text,BASE+'?ref=22385')
        (Path(__file__).parents[1]/'test-results/chapter-ngoc-qr.png').write_bytes(response.get_body())

    def test_card_and_svg_preserve_query_after_path(self):
        repo=Mock();repo.get_referral_chapter.return_value=CHAPTER
        with patch('function_app._repository',return_value=repo):
            body=workshop_qr_card(self.request()).get_body().decode()
            svg=workshop_qr_download(self.request('svg')).get_body().decode()
        for url in [BASE+'?ref=22385',BASE+'/workshop-qr?ref=22385',BASE+'/workshop-qr/png?ref=22385']:
            self.assertIn(url,body)
        self.assertNotIn('?ref=22385/png',body)
        self.assertIn('National Go Center (NGOC)',body)
        self.assertIn(BASE+'?ref=22385',svg)
        self.assertNotIn('__',body)

    def test_unknown_chapter_download_and_card_are_not_generic_codes(self):
        repo=Mock();repo.get_referral_chapter.return_value=None
        with patch('function_app._repository',return_value=repo):
            self.assertEqual(workshop_qr_card(self.request()).status_code,404)
            self.assertEqual(workshop_qr_download(self.request('png')).status_code,404)

    def test_api_and_registration_pass_chapter_to_repository(self):
        repo=Mock();repo.list_referral_chapters.return_value=[CHAPTER];repo.register_free.return_value={'registration_id':'test'}
        request=func.HttpRequest(method='POST',url=BASE,route_params={'slug':'learn-go-in-an-hour'},body=json.dumps({
            'contact':{'first_name':'Test','last_name':'Person','email':'test@example.com'},
            'occurrence_ids':[1],'idempotency_key':'chapter-test','referral_chapter_id':'22385'}).encode())
        with patch('function_app._repository',return_value=repo):
            self.assertEqual(json.loads(workshop_referral_chapters(self.request()).get_body())['chapters'],[CHAPTER])
            self.assertEqual(events_register_free(request).status_code,201)
        self.assertEqual(repo.register_free.call_args.kwargs['referral_chapter_id'],'22385')

    def test_invalid_referrals_never_reach_sql(self):
        for value in [None,True,False,0,-1,1.5,'',[],{},'1 OR 1=1','١٢٣','2147483648','9'*2000]:
            cursor=Mock()
            self.assertIsNone(SqlEventRepository._referral_chapter(cursor,value))
            cursor.execute.assert_not_called()

    def test_chapter_id_lookup_is_parameterized(self):
        cursor=Mock();cursor.description=[('chapter_id',),('chapter_code',),('chapter_name',)]
        cursor.fetchone.return_value=(22385,'NGOC','National Go Center')
        self.assertEqual(SqlEventRepository._referral_chapter(cursor,'22385'),CHAPTER)
        self.assertEqual(cursor.execute.call_args.args[1],22385)
