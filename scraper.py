import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from geocodio import GeocodioClient
import geohash2
import argparse
import logging
from faunadb import query as q
from faunadb.client import FaunaClient

# Initialize logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Geocodio client
api_key = os.getenv('GEOCODIO_API_KEY')  # Use the environment variable for the API key
if not api_key:
    raise ValueError("Geocodio API key not found. Set it as an environment variable 'GEOCODIO_API_KEY'")
client = GeocodioClient(api_key)

# Initialize FaunaDB client
fauna_secret = os.getenv('FAUNADB_SECRET')
if not fauna_secret:
    raise ValueError("FaunaDB secret key not found. Set it as an environment variable 'FAUNADB_SECRET'")
fauna_client = FaunaClient(secret=fauna_secret)

# Create collections and indexes if they don't exist
def create_collections_and_indexes():
    try:
        # Create collections
        fauna_client.query(q.create_collection({"name": "licenses"}))
        fauna_client.query(q.create_collection({"name": "geocode_cache"}))

        # Create indexes
        fauna_client.query(q.create_index({
            "name": "geocode_cache_by_address",
            "source": q.collection("geocode_cache"),
            "terms": [{"field": ["data", "address"]}],
            "unique": True
        }))
        fauna_client.query(q.create_index({
            "name": "licenses_by_license_number_notification_date_license_type",
            "source": q.collection("licenses"),
            "terms": [
                {"field": ["data", "license_number"]},
                {"field": ["data", "notification_date"]},
                {"field": ["data", "license_type"]}
            ],
            "unique": True
        }))
        fauna_client.query(q.create_index({
            "name": "licenses_by_creation_date",
            "source": q.collection("licenses"),
            "values": [
                {"field": ["ref"]},
                {"field": ["data", "creation_date"]}
            ]
        }))
    except Exception as e:
        logging.error(f"Error creating collections or indexes: {e}")

def geocode_addresses_batch(addresses):
    logging.debug(f"Geocoding {len(addresses)} addresses...")

    # Batch check if addresses already exist in the cache
    cache_lookup_operations = [q.get(q.match(q.index("geocode_cache_by_address"), address)) for address in addresses]

    existing_results = {}
    try:
        cache_results = fauna_client.query(q.map_(lambda x: x, cache_lookup_operations))
        for address, result in zip(addresses, cache_results):
            if result:
                existing_results[address] = (
                    result['data']['latitude'],
                    result['data']['longitude'],
                    result['data']['geohash'],
                    result['data']['zipcode'],
                    result['data']['formatted_address']
                )
    except Exception as e:
        logging.error(f"Error checking cache for addresses: {e}")

    # Addresses to geocode via API
    addresses_to_geocode = [address for address in addresses if address not in existing_results]

    if addresses_to_geocode:
        logging.debug(f"{len(addresses_to_geocode)} addresses not found in cache. Making batch geocode request...")
        geocode_results = client.batch_geocode(addresses_to_geocode)
        for address, result in zip(addresses_to_geocode, geocode_results):
            if result and result['results']:
                location = result['results'][0]['location']
                lat = location['lat']
                lng = location['lng']
                geohash_code = geohash2.encode(lat, lng)
                zipcode = result['results'][0]['address_components'].get('zip')
                formatted_address = result['results'][0].get('formatted_address', '')
                existing_results[address] = (lat, lng, geohash_code, zipcode, formatted_address)

                # Store in cache
                try:
                    fauna_client.query(q.create(q.collection("geocode_cache"), {
                        "data": {
                            "address": address,
                            "latitude": lat,
                            "longitude": lng,
                            "geohash": geohash_code,
                            "zipcode": zipcode,
                            "formatted_address": formatted_address,
                            "creation_date": q.now()
                        }
                    }))
                except Exception as e:
                    logging.error(f"Error caching geocode data for address {address}: {e}")
    else:
        logging.debug("All addresses found in the cache")
    return existing_results

def fetch_webpage():
    url = 'https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp'
    logging.debug(f"Fetching webpage content from {url}...")
    response = requests.get(url)
    response.raise_for_status()
    return response.content

def parse_html(html_content):
    logging.debug("Parsing HTML content...")
    soup = BeautifulSoup(html_content, 'html.parser')
    rows = soup.select("tbody[width='100%']")
    data = []
    for row in rows:
        labels = [label.text.strip().rstrip(':') for label in row.select("td[style]")]
        values = [value.text.strip() for value in row.select("td:not([style])")]
        if labels and values:
            row_data = dict(zip(labels, values))
            data.append(row_data)
    logging.debug(f"Parsed {len(data)} rows from HTML content.")
    return data

def get_notification_date(entry):
    try:
        date_str = entry.get('Notification Date') or entry.get('Approved Date') or entry.get('Discontinued Date')
        return datetime.strptime(date_str, '%m/%d/%Y').date() if date_str else None
    except Exception as e:
        logging.error(f"Error parsing notification date {date_str}: {e}")
        return None

def upsert_data(data, geocode_results):
    logging.debug("Upserting data into the database...")
    for entry in data:
        address = entry.get('Business Location') or entry.get('New Business Location')
        lat, lng, geohash_code, zipcode, formatted_address = geocode_results.get(address, (None, None, None, None, None))
        notification_date = get_notification_date(entry)

        # Ensure notification_date is a string if it's not None
        notification_date_str = notification_date.strftime('%Y-%m-%d') if notification_date else None

        # Prepare the document
        license_data = {
            "notification_date": notification_date_str,  # Use the string format
            "current_business_name": entry.get('Current Business Name'),
            "new_business_name": entry.get('New Business Name'),
            "business_location": entry.get('Business Location') or entry.get('New Business Location'),
            "current_applicants": entry.get('Current Applicant(s)'),
            "new_applicants": entry.get('New Applicant(s)'),
            "license_type": entry.get('License Type'),
            "application_type": entry.get('Application Type'),
            "license_number": entry.get('License Number'),
            "contact_phone": entry.get('Contact Phone'),
            "latitude": lat,
            "longitude": lng,
            "geohash": geohash_code,
            "zipcode": zipcode,
            "formatted_address": formatted_address,
            "last_updated_date": q.now(),
            "creation_date": q.now(),
            "business_name": entry.get('Business Name'),
            "applicants": entry.get('Applicant(s)'),
        }
        #logging.debug(license_data)
        # Upsert in FaunaDB
        try:
            fauna_client.query(q.if_(
                q.exists(q.match(q.index("licenses_by_license_number_notification_date_license_type"), entry.get('License Number'), notification_date, entry.get('License Type'))),
                q.update(q.select("ref", q.get(q.match(q.index("licenses_by_license_number_notification_date_license_type"), entry.get('License Number'), notification_date, entry.get('License Type')))), {"data": license_data}),
                q.create(q.collection("licenses"), {"data": license_data})
            ))
        except Exception as e:
            logging.error(f"Error upserting data for license number {entry.get('License Number')}: {e}. data: {license_data}")

    logging.debug("Data upsertion completed.")


def main(limit):
    html_content = fetch_webpage()
    data = parse_html(html_content)
    if limit:
        logging.debug(f"Limiting the number of rows to {limit}")
        data = data[:limit]
    addresses = [entry.get('Business Location') or entry.get('New Business Location') for entry in data]
    geocode_results = geocode_addresses_batch(addresses)
    upsert_data(data, geocode_results)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape a webpage and store the data in FaunaDB.')
    parser.add_argument('--limit', type=int, default=None, help='Limit the number of rows processed for testing')
    parser.add_argument('--create-tables', action="store_true", default=None, help='creates tables in database')
    args = parser.parse_args()
    if (args.create_tables):
        create_collections_and_indexes()
    else:
        main(args.limit)
