"""Verify the invitation artwork, registration target and download responses."""
from io import BytesIO
from pathlib import Path
import os
import unittest
from unittest.mock import patch
from xml.etree import ElementTree

from PIL import Image
from workshop_qr import badge_png, badge_svg, CAPTION, REGISTRATION_URL


class WorkshopQrTests(unittest.TestCase):
    def test_public_card_uses_canonical_links_without_database_or_signin(self):
        import azure.functions as func
        from function_app import workshop_qr_card,workshop_qr_share_script
        req=func.HttpRequest(method='GET',url='https://ignored.example/events/learn-go-in-an-hour/workshop-qr',
            route_params={'slug':'learn-go-in-an-hour'},body=b'')
        with patch.dict(os.environ,{'EVENTS_PUBLIC_BASE_URL':'https://aga-events.azurewebsites.net'}),patch('function_app._repository',side_effect=AssertionError('Public card must not read registrations')):
            response=workshop_qr_card(req)
        self.assertEqual(response.status_code,200)
        body=response.get_body().decode()
        self.assertIn('data-share-url="'+REGISTRATION_URL+'/workshop-qr"',body)
        self.assertIn('href="'+REGISTRATION_URL+'"',body)
        self.assertNotIn('__CARD_URL__',body)
        self.assertNotIn('Sign in',body)
        self.assertIn('text/html',response.headers['Content-Type'])
        self.assertEqual(workshop_qr_share_script(req).status_code,200)
        req=func.HttpRequest(method='GET',url='https://ignored.example/events/other/workshop-qr',route_params={'slug':'other'},body=b'')
        self.assertEqual(workshop_qr_card(req).status_code,404)

    def test_rehearsal_card_and_download_keep_test_registration_target(self):
        import azure.functions as func
        import zxingcpp
        from function_app import workshop_qr_card,workshop_qr_download
        slug='learn-go-in-an-hour-rehearsal'
        url=REGISTRATION_URL+'-rehearsal'
        req=func.HttpRequest(method='GET',url=url+'/workshop-qr',route_params={'slug':slug},body=b'')
        with patch.dict(os.environ,{'EVENTS_PUBLIC_BASE_URL':'https://aga-events.azurewebsites.net'}):
            card=workshop_qr_card(req)
            download=workshop_qr_download(func.HttpRequest(method='GET',url=url+'/workshop-qr/png',route_params={'slug':slug,'format':'png'},body=b''))
        self.assertEqual(card.status_code,200)
        self.assertIn(('data-share-url="'+url+'/workshop-qr"').encode(),card.get_body())
        self.assertIn('TEST — Free Beginners Go Workshop'.encode(),card.get_body())
        decoded=zxingcpp.read_barcode(Image.open(BytesIO(download.get_body())))
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded.text,url)

    def test_invitation_wording_and_vector_accessibility(self):
        self.assertEqual(CAPTION,'Free Beginners Online Workshop Learn Go in an hour - Register here.')
        svg=ElementTree.fromstring(badge_svg())
        ns={'s':'http://www.w3.org/2000/svg'}
        self.assertEqual(svg.find('s:title',ns).text,CAPTION)
        self.assertEqual(' '.join(e.text for e in svg.findall('.//s:textPath',ns)),CAPTION)
        self.assertIn(REGISTRATION_URL,svg.find('s:desc',ns).text)

    def test_downloads_use_configured_permanent_registration_origin(self):
        import azure.functions as func
        from function_app import workshop_qr_download
        for format,content_type in [('svg','image/svg+xml'),('png','image/png')]:
            req=func.HttpRequest(method='GET',url=f'https://ignored.example/events/learn-go-in-an-hour/workshop-qr/{format}',
                route_params={'slug':'learn-go-in-an-hour','format':format},body=b'')
            with patch.dict(os.environ,{'EVENTS_PUBLIC_BASE_URL':'https://aga-events.azurewebsites.net'}):
                response=workshop_qr_download(req)
            self.assertEqual(response.status_code,200)
            self.assertIn(content_type,response.headers['Content-Type'])
            self.assertEqual(response.headers['Content-Disposition'],f'attachment; filename="free-beginners-workshop-qr.{format}"')
            if format=='svg':
                self.assertIn(REGISTRATION_URL.encode(),response.get_body())
                self.assertEqual(ElementTree.fromstring(response.get_body()).attrib['width'],'360')
            else:
                fixture=Path(__file__).resolve().parents[1]/'test-results/workshop-phone-qr.png'
                fixture.parent.mkdir(exist_ok=True)
                fixture.write_bytes(response.get_body())
                image=Image.open(BytesIO(response.get_body()))
                self.assertEqual(image.size,(360,360))
                import zxingcpp
                for width in (360,320,280):
                    decoded=zxingcpp.read_barcode(image.resize((width,width)))
                    self.assertIsNotNone(decoded,f'Phone QR did not decode at {width}px')
                    self.assertEqual(decoded.text,REGISTRATION_URL)

    def test_full_png_scans_at_original_and_smaller_sizes(self):
        try:
            import zxingcpp
        except ImportError:
            self.skipTest('Install zxing-cpp to independently decode the artwork.')
        original=Image.open(BytesIO(badge_png()))
        self.assertEqual(original.size,(1200,1200))
        for size in (1200,600,300):
            rendered=original.resize((size,size))
            result=zxingcpp.read_barcode(rendered)
            self.assertIsNotNone(result,f'Badge did not decode at {size}px')
            self.assertEqual(result.text,REGISTRATION_URL)
