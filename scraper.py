import os
import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime, timedelta
from geocodio import GeocodioClient
import geohash2
import argparse
import logging

# Initialize logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Geocodio client
api_key = os.getenv('GEOCODIO_API_KEY')  # Use the environment variable for the API key
if not api_key:
    raise ValueError("Geocodio API key not found. Set it as an environment variable 'GEOCODIO_API_KEY'")
client = GeocodioClient(api_key)

def geocode_addresses_batch(addresses):
    logging.debug(f"Geocoding {len(addresses)} addresses using batch API...")
    # Check if addresses already exist in the cache
    existing_results = {}
    cursor.execute("SELECT address, latitude, longitude, geohash, zipcode, formatted_address FROM geocode_cache WHERE address IN ({})"
                   .format(','.join(['?'] * len(addresses))), addresses)
    for row in cursor.fetchall():
        existing_results[row[0]] = row[1:]

    # Addresses to geocode via API
    addresses_to_geocode = [address for address in addresses if address not in existing_results]
    if addresses_to_geocode:
        logging.debug(f"{len(addresses_to_geocode)} addresses not found in cache. Making batch geocode request...")
        geocode_results = client.batch_geocode(addresses_to_geocode)
        if geocode_results:
            for address, result in zip(addresses_to_geocode, geocode_results):
                if result and result['results']:
                    location = result['results'][0]['location']
                    lat = location['lat']
                    lng = location['lng']
                    geohash_code = geohash2.encode(lat, lng)
                    zipcode = result['results'][0]['address_components'].get('zip')
                    formatted_address = result['results'][0].get('formatted_address', '')

                    # Store in cache
                    cursor.execute('''
                    INSERT OR REPLACE INTO geocode_cache (address, latitude, longitude, geohash, zipcode, formatted_address)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''', (address, lat, lng, geohash_code, zipcode, formatted_address))
                    conn.commit()
                    
                    existing_results[address] = (lat, lng, geohash_code, zipcode, formatted_address)
        logging.debug("Batch geocode request completed.")
    return existing_results

def fetch_webpage(url):
    logging.debug(f"Fetching webpage: {url}")
    response = requests.get(url)
    response.raise_for_status()
    return response.text

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
    return entry.get('Notification Date') or entry.get('Approved Date') or entry.get('Discontinued Date')

def upsert_data(data, geocode_results):
    logging.debug("Upserting data into the database...")
    for entry in data:
        address = entry.get('Business Location') or entry.get('New Business Location')
        lat, lng, geohash_code, zipcode, formatted_address = geocode_results.get(address, (None, None, None, None, None))
        notification_date = get_notification_date(entry)
        cursor.execute('''
        INSERT INTO licenses (
            notification_date, current_business_name, new_business_name, business_location, current_applicants, new_applicants,
            license_type, application_type, license_number, contact_phone, latitude, longitude, geohash, zipcode,
            formatted_address, last_updated_date, creation_date, business_name, applicants
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (license_number, notification_date, license_type) DO UPDATE SET
            current_business_name=excluded.current_business_name,
            new_business_name=excluded.new_business_name,
            business_location=excluded.business_location,
            current_applicants=excluded.current_applicants,
            new_applicants=excluded.new_applicants,
            application_type=excluded.application_type,
            contact_phone=excluded.contact_phone,
            latitude=excluded.latitude,
            longitude=excluded.longitude,
            geohash=excluded.geohash,
            zipcode=excluded.zipcode,
            formatted_address=excluded.formatted_address,
            last_updated_date=excluded.last_updated_date,
            business_name=excluded.business_name,
            applicants=excluded.applicants
        ''', (
            notification_date,
            entry.get('Current Business Name'),
            entry.get('New Business Name'),
            entry.get('Business Location') or entry.get('New Business Location'),
            entry.get('Current Applicant(s)'),
            entry.get('New Applicant(s)'),
            entry.get('License Type'),
            entry.get('Application Type'),
            entry.get('License Number'),
            entry.get('Contact Phone'),
            lat,
            lng,
            geohash_code,
            zipcode,
            formatted_address,
            datetime.now().isoformat(),
            datetime.now().isoformat(),
            entry.get('Business Name'),
            entry.get('Applicant(s)')
        ))
    logging.debug("Data upsertion completed.")

def delete_old_data():
    logging.debug("Deleting data older than 6 months...")
    six_months_ago = datetime.now() - timedelta(days=180)
    cursor.execute('''
    DELETE FROM licenses WHERE creation_date < ?
    ''', (six_months_ago.isoformat(),))
    logging.debug("Old data deletion completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fetch and store WSLB licensing data.')
    parser.add_argument('--limit', type=int, default=None, help='Limit the number of rows processed for testing')
    args = parser.parse_args()

    # Ensure the data directory exists
    os.makedirs('data', exist_ok=True)

    # Connect to SQLite database
    logging.debug("Connecting to SQLite database...")
    conn = sqlite3.connect('data/wslcb_data.db')
    cursor = conn.cursor()

    # Create tables if they don't exist
    logging.debug("Creating tables if they don't exist...")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS licenses (
        id INTEGER PRIMARY KEY,
        notification_date TEXT,
        current_business_name TEXT,
        new_business_name TEXT,
        business_location TEXT,
        current_applicants TEXT,
        new_applicants TEXT,
        license_type TEXT,
        application_type TEXT,
        license_number TEXT,
        contact_phone TEXT,
        latitude REAL,
        longitude REAL,
        geohash TEXT,
        zipcode TEXT,
        formatted_address TEXT,
        last_updated_date TEXT,
        creation_date TEXT,
        business_name TEXT,
        applicants TEXT,
        UNIQUE(license_number, notification_date, license_type)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS geocode_cache (
        address TEXT PRIMARY KEY,
        latitude REAL,
        longitude REAL,
        geohash TEXT,
        zipcode TEXT,
        formatted_address TEXT
    )
    ''')

    # Fetch and parse the webpage
    url = 'https://licensinginfo.lcb.wa.gov/EntireStateWeb.asp'
    html_content = fetch_webpage(url)
    data = parse_html(html_content)

    # Limit the number of rows processed if the --limit flag is set
    if args.limit:
        logging.debug(f"Limiting the number of rows to {args.limit}")
        data = data[:args.limit]

    # Extract addresses and geocode them
    addresses = [entry.get('Business Location') or entry.get('New Business Location') for entry in data if entry.get('Business Location') or entry.get('New Business Location')]
    geocode_results = geocode_addresses_batch(addresses)

    # Insert or update data into SQLite database
    upsert_data(data, geocode_results)

    # Delete data older than 6 months
    delete_old_data()

    # Commit changes and close the connection
    logging.debug("Committing changes to the database...")
    conn.commit()
    conn.close()
    logging.debug("Database connection closed.")
