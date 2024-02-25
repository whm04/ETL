# tests/test_api.py
import unittest,os 
import sys  
current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

import unittest
from rest_api.__main__ import app as tested_app

class TestAPI(unittest.TestCase):
    def setUp(self):
        self.app = tested_app.test_client()

    def test_read_first_chunk(self):
        response = self.app.get('/read/first-chunk')
        self.assertEqual(response.status_code, 200)
        response = response.json  # Assuming the response is JSON
        # Add more assertions for response content
        self.assertIsInstance(response, dict)
        self.assertEqual(len(response['data']), 10)  # Assuming the response contains 10 items
    
    def test_not_found_endpoint(self):
        response = self.app.get('/read')
        self.assertEqual(response.status_code, 404)
        

if __name__ == '__main__':
    unittest.main()

