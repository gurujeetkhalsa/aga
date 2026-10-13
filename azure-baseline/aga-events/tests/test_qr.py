"""Verify the invitation artwork, registration target and download responses."""
from io import BytesIO
import os
import unittest
from unittest.mock import patch
from xml.etree import ElementTree

from PIL import Image
from workshop_qr import badge_png, badge_svg, CAPTION, REGISTRATION_URL


class WorkshopQrTests(unittest.TestCase):
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
            if format=='svg':self.assertIn(REGISTRATION_URL.encode(),response.get_body())

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
