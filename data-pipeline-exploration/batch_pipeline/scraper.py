"""
Product Scraper - Simulates extracting product data from external sources
In a real scenario, this would scrape e-commerce sites or call APIs

For demo purposes, generates realistic messy data
"""

import random
import json
from datetime import datetime
from typing import List, Dict

class ProductScraper:
    """Simulates scraping product data with realistic quality issues"""
    
    def __init__(self):
        self.product_templates = [
            {"category": "Electronics", "names": ["Laptop", "Mouse", "Keyboard", "Monitor", "Webcam"]},
            {"category": "Books", "names": ["Python Guide", "SQL Mastery", "Data Engineering", "Cloud Computing"]},
            {"category": "Home", "names": ["Coffee Maker", "Blender", "Toaster", "Mixer"]}
        ]
        
        self.sources = ["vendor_a", "vendor_b", "vendor_c"]
    
    def generate_product_id(self) -> str:
        """Generate product ID"""
        return f"P{random.randint(1000, 9999)}"
    
    def generate_messy_data(self, num_products: int = 20) -> List[Dict]:
        """
        Generate product data with realistic quality issues
        
        Issues introduced:
        - Missing values (nulls)
        - Inconsistent price formats
        - Negative stock values
        - Duplicate records
        - Extra whitespace
        - Invalid data types
        """
        products = []
        
        for _ in range(num_products):
            # Pick random category
            category = random.choice(self.product_templates)
            
            # Base product
            product = {
                "product_id": self.generate_product_id(),
                "name": random.choice(category["names"]),
                "category": category["category"],
                "price": round(random.uniform(10, 500), 2),
                "stock": random.randint(0, 100),
                "source": random.choice(self.sources),
                "scraped_at": datetime.now().isoformat()
            }
            
            # Introduce data quality issues (30% of records)
            issue_type = random.random()
            
            if issue_type < 0.05:  # 5% missing product name
                product["name"] = None
                
            elif issue_type < 0.10:  # 5% missing price
                del product["price"]
                
            elif issue_type < 0.15:  # 5% price as string with $ and commas
                product["price"] = f"${product['price']:,.2f}"
                
            elif issue_type < 0.18:  # 3% negative stock
                product["stock"] = -random.randint(1, 10)
                
            elif issue_type < 0.20:  # 2% stock as string
                product["stock"] = str(product["stock"])
                
            elif issue_type < 0.23:  # 3% extra whitespace in name
                if product["name"]:
                    product["name"] = f"  {product['name']}  "
            
            elif issue_type < 0.25:  # 2% price as word
                product["price"] = "CALL"
                
            elif issue_type < 0.27:  # 2% missing product_id
                del product["product_id"]
            
            products.append(product)
        
        # Add intentional duplicates (5% of total)
        num_duplicates = max(1, int(num_products * 0.05))
        for _ in range(num_duplicates):
            products.append(random.choice(products).copy())
        
        return products
    
    def scrape_source(self, source_name: str, num_products: int = 20) -> List[Dict]:
        """
        Simulate scraping from a specific source
        
        Args:
            source_name: Name of the vendor/source
            num_products: Number of products to generate
            
        Returns:
            List of product dictionaries
        """
        products = self.generate_messy_data(num_products)
        
        # Override source for all products
        for product in products:
            product["source"] = source_name
        
        return products


if __name__ == "__main__":
    # Test the scraper
    scraper = ProductScraper()
    
    print("Generating sample messy product data...\n")
    products = scraper.scrape_source("test_vendor", num_products=10)
    
    print(f"Generated {len(products)} products")
    print("\nSample products:")
    for i, product in enumerate(products[:5], 1):
        print(f"\n{i}. {json.dumps(product, indent=2)}")
    
    print("\n✅ Scraper working!")