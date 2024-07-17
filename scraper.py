import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from geocodio import GeocodioClient
import geohash2
import argparse
import logging
from xata.client import XataClient
from xata.helpers import to_rfc339
import hashlib

# Initialize logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Geocodio client
api_key = os.getenv('GEOCODIO_API_KEY')
if not api_key:
    raise ValueError("Geocodio API key not found. Set it as an environment variable 'GEOCODIO_API_KEY'")
client = GeocodioClient(api_key)

# Initialize Xata client
xata_api_key = os.getenv('XATA_API_KEY')
xata_db_url = os.getenv('XATA_DB_URL')
if not xata_api_key or not xata_db_url:
    raise ValueError("Xata API key or database URL not found. Set them as environment variables 'XATA_API_KEY' and 'XATA_DB_URL'")
xata_client = XataClient(api_key=xata_api_key, db_url=xata_db_url)

def geocode_addresses_batch(addresses):
    logging.debug(f"Geocoding {len(addresses)} addresses...")

    # Query all addresses at once
    try:
        cache_results = xata_client.data().query('geocode_cache', {
            "filter": {
                "$any" : [
                    {"address": { "$is": a }}
                    for a in addresses
                ],
            },
            "page": {
                "size": 1000
            },
            "columns": ["address", "latitude", "longitude", "geohash", "zipcode", "formatted_address"]
        })
        
        if not cache_results.is_success():
            raise Exception(f"Failed to query geocode cache: {cache_results.error_message}")

        # Create a dictionary of existing results
        existing_results = {
            record["address"]: (
                record["latitude"],
                record["longitude"],
                record["geohash"],
                record["zipcode"],
                record["formatted_address"]
            )
            for record in cache_results["records"]
        }
    except Exception as e:
        logging.error(f"Error fetching cache for addresses: {e}")
        existing_results = {}

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
                    res = xata_client.records().upsert("geocode_cache", hash(address), {
                        "address": address,
                        "latitude": lat,
                        "longitude": lng,
                        "geohash": geohash_code,
                        "zipcode": zipcode,
                        "formatted_address": formatted_address
                    })
                    if res.is_success() == False:
                        logging.error(f"Error caching geocode data for address {address}: {res.error_message}")
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
        return datetime.strptime(date_str, '%m/%d/%Y') if date_str else None
    except Exception as e:
        logging.error(f"Error parsing notification date {date_str}: {e}")
        return None

def upsert_data(data, geocode_results):
    logging.debug("Upserting data into the database...")
    for entry in data:
        address = entry.get('Business Location') or entry.get('New Business Location')
        lat, lng, geohash_code, zipcode, formatted_address = geocode_results.get(address, (None, None, None, None, None))
        notification_date = to_rfc339(get_notification_date(entry))

        license_data = {
            "notification_date": notification_date,
            "current_business_name": entry.get('Current Business Name'),
            "new_business_name": entry.get('New Business Name'),
            "business_location": address,
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
            "business_name": entry.get('Business Name'),
            "applicants": entry.get('Applicant(s)')
        }

        try:
            existing_record = xata_client.data().query("licenses", {
                "filter": {
                    "$all": [
                        {"license_number": entry.get('License Number')},
                        {"notification_date": notification_date},
                        {"license_type": entry.get('License Type')}
                    ]
                }
            })

            if existing_record["records"]:
                xata_client.records().update("licenses", existing_record["records"][0]["id"], license_data)
            else:
                xata_client.records().insert("licenses", license_data)
        except Exception as e:
            logging.error(f"Error upserting data for license number {entry.get('License Number')}: {e}")

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
    parser = argparse.ArgumentParser(description='Scrape a webpage and store the data in Xata.')
    parser.add_argument('--limit', type=int, default=None, help='Limit the number of rows processed for testing')
    args = parser.parse_args()
    
    main(args.limit)