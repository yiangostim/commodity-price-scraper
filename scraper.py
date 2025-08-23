#!/usr/bin/env python3
"""
Commodity Price Scraper for Business Insider Markets
Scrapes precious metals, energy, industrial metals, and agriculture commodity prices
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from datetime import datetime, timezone
import os
import time

def scrape_commodity_prices():
    """
    Scrape commodity prices from Business Insider Markets
    Returns a dictionary with all commodity data
    """
    
    url = "https://markets.businessinsider.com/commodities"
    
    # Headers to mimic a browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    }
    
    try:
        print(f"Fetching data from {url}")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all commodity tables
        tables = soup.find_all('table', class_='table')
        
        all_commodities = []
        categories = ['Precious Metals', 'Energy', 'Industrial Metals', 'Agriculture']
        
        for i, table in enumerate(tables[:4]):  # Only process first 4 tables
            if i >= len(categories):
                break
                
            category = categories[i]
            print(f"Processing {category} table...")
            
            # Find all rows in the table body
            rows = table.find('tbody').find_all('tr') if table.find('tbody') else []
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 6:
                    # Extract commodity data
                    name_cell = cells[0].find('a')
                    commodity_name = name_cell.text.strip() if name_cell else cells[0].text.strip()
                    
                    # Extract price (remove any formatting)
                    price_span = cells[1].find('span', class_='push-data')
                    price = price_span.text.strip() if price_span else cells[1].text.strip()
                    
                    # Extract percentage change
                    pct_span = cells[2].find('span', class_='push-data')
                    pct_change = pct_span.text.strip() if pct_span else cells[2].text.strip()
                    
                    # Extract absolute change
                    abs_span = cells[3].find('span', class_='push-data')
                    abs_change = abs_span.text.strip() if abs_span else cells[3].text.strip()
                    
                    # Extract unit
                    unit = cells[4].text.strip()
                    
                    # Extract date/time
                    date_span = cells[5].find('span', class_='push-data')
                    date_time = date_span.text.strip() if date_span else cells[5].text.strip()
                    
                    # Determine color/trend from CSS classes
                    trend = 'neutral'
                    if pct_span:
                        if 'colorGreen' in pct_span.get('class', []):
                            trend = 'up'
                        elif 'colorRed' in pct_span.get('class', []):
                            trend = 'down'
                    
                    commodity_data = {
                        'category': category,
                        'name': commodity_name,
                        'price': price,
                        'percentage_change': pct_change,
                        'absolute_change': abs_change,
                        'unit': unit,
                        'market_time': date_time,
                        'trend': trend
                    }
                    
                    all_commodities.append(commodity_data)
                    print(f"  - {commodity_name}: {price} ({pct_change})")
        
        return all_commodities
        
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return []
    except Exception as e:
        print(f"Error parsing data: {e}")
        return []

def save_data(commodities_data):
    """
    Save the scraped data to a single CSV file (append mode)
    """
    if not commodities_data:
        print("No data to save")
        return
    
    # Create European-style timestamp: 23/08/2025 14:06
    now_utc = datetime.now(timezone.utc)
    scrape_timestamp = now_utc.strftime("%d/%m/%Y %H:%M")
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Prepare CSV data
    csv_data = []
    for commodity in commodities_data:
        csv_row = {
            'timestamp': scrape_timestamp,
            'category': commodity['category'],
            'name': commodity['name'],
            'price': commodity['price'],
            'percentage_change': commodity['percentage_change'],
            'absolute_change': commodity['absolute_change'],
            'unit': commodity['unit'],
            'market_time': commodity['market_time'],
            'trend': commodity['trend']
        }
        csv_data.append(csv_row)
    
    df = pd.DataFrame(csv_data)
    
    # Define the main CSV file
    csv_filename = 'data/commodity_prices.csv'
    
    # Check if file exists to determine if we need headers
    file_exists = os.path.exists(csv_filename)
    
    # Append to CSV file (or create if doesn't exist)
    df.to_csv(csv_filename, mode='a', header=not file_exists, index=False)
    
    if file_exists:
        print(f"Appended {len(csv_data)} commodities to {csv_filename}")
    else:
        print(f"Created {csv_filename} with {len(csv_data)} commodities")
    
    # Also save as latest JSON for API purposes
    latest_data = {
        'last_updated': scrape_timestamp,
        'total_commodities': len(commodities_data),
        'commodities': commodities_data
    }
    
    with open('data/latest_prices.json', 'w') as f:
        json.dump(latest_data, f, indent=2)
    print("Updated latest_prices.json")

def main():
    """
    Main function to run the scraper
    """
    now_utc = datetime.now(timezone.utc)
    print(f"Starting commodity price scrape at {now_utc.strftime('%d/%m/%Y %H:%M')}")
    
    # Scrape the data
    commodities = scrape_commodity_prices()
    
    if commodities:
        print(f"\nSuccessfully scraped {len(commodities)} commodities")
        
        # Save the data
        save_data(commodities)
        
        print(f"\nScrape completed successfully at {now_utc.strftime('%d/%m/%Y %H:%M')}!")
    else:
        print("No data was scraped. Please check the website structure or network connection.")
        exit(1)

if __name__ == "__main__":
    main()
