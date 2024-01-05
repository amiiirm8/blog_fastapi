
import unittest
from fastapi.testclient import TestClient
from main import app

class TestMain(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_insert_content(self):
        response = self.client.post("/insert", json={"title": "Test Title", "text": "Test Text", "author": "Test Author"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"message": "Content added to the queue for processing"})

    def test_list_content(self):
        response = self.client.get("/list")
        self.assertEqual(response.status_code, 200)
        # Add more assertions based on the expected response format

    def test_search_content(self):
        response = self.client.get("/search/Test")
        self.assertEqual(response.status_code, 200)
        # Add more assertions based on the expected response format

    def test_insert_bulk(self):
        response = self.client.post("/insert_bulk", json=[{"title": "Bulk Title", "text": "Bulk Text", "author": "Bulk Author"}])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"message": "Content added to the queue for processing in bulk"})

if __name__ == "__main__":
    unittest.main()
