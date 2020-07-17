from django.test import Client as client
import pytest

@pytest.mark.django_db
def test_rainfall_garr_response(client):
    response = client.get('/rainfall/v2/garr/')
    assert response.status_code == 200

@pytest.mark.django_db
def test_tracts_api_response(client):
    response = client.get('/rainfall/v2/gauge/')
    assert response.status_code == 200