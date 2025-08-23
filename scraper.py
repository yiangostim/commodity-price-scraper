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
    Save the scraped data to multiple formats
    """
    if not commodities_data:
        print("No data to save")
        return
    
    # Create timestamp for this scrape
    scrape_timestamp = datetime.now(timezone.utc).isoformat()
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Create the final data structure
    data_structure = {
        'scrape_timestamp_utc': scrape_timestamp,
        'total_commodities': len(commodities_data),
        'categories': len(set(item['category'] for item in commodities_data)),
        'commodities': commodities_data
    }
    
    # Save as JSON (detailed data)
    json_filename = f'data/commodity_prices_{datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")}.json'
    with open(json_filename, 'w') as f:
        json.dump(data_structure, f, indent=2)
    print(f"Saved detailed data to {json_filename}")
    
    # Save as CSV (flattened data)
    df = pd.DataFrame(commodities_data)
    df['scrape_timestamp_utc'] = scrape_timestamp
    csv_filename = f'data/commodity_prices_{datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")}.csv'
    df.to_csv(csv_filename, index=False)
    print(f"Saved CSV data to {csv_filename}")
    
    # Save latest data (overwrite each time)
    with open('data/latest_prices.json', 'w') as f:
        json.dump(data_structure, f, indent=2)
    print("Updated latest_prices.json")
    
    df.to_csv('data/latest_prices.csv', index=False)
    print("Updated latest_prices.csv")
    
    # Create summary
    summary = {
        'last_updated_utc': scrape_timestamp,
        'total_commodities': len(commodities_data),
        'categories_summary': {}
    }
    
    for category in set(item['category'] for item in commodities_data):
        category_items = [item for item in commodities_data if item['category'] == category]
        summary['categories_summary'][category] = {
            'count': len(category_items),
            'commodities': [item['name'] for item in category_items]
        }
    
    with open('data/summary.json', 'w') as f:
        json.dump(summary, f, indent=2)
    print("Updated summary.json")

def main():
    """
    Main function to run the scraper
    """
    print(f"Starting commodity price scrape at {datetime.now(timezone.utc).isoformat()}")
    
    # Scrape the data
    commodities = scrape_commodity_prices()
    
    if commodities:
        print(f"\nSuccessfully scraped {len(commodities)} commodities")
        
        # Save the data
        save_data(commodities)
        
        print("\nScrape completed successfully!")
    else:
        print("No data was scraped. Please check the website structure or network connection.")
        exit(1)

if __name__ == "__main__":
    main()
