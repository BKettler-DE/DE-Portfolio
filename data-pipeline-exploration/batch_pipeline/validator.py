"""
Batch Data Validator
Validates product data with comprehensive quality checks
"""

import json
import re
from typing import Dict, List, Tuple
from datetime import datetime

class ProductValidator:
    """Validates product data and categorizes as clean or quarantine"""
    
    def __init__(self):
        self.validation_stats = {
            "total": 0,
            "valid": 0,
            "invalid": 0,
            "duplicates": 0,
            "issues_breakdown": {}
        }
        
        self.seen_product_ids = set()
    
    def validate_product(self, product: Dict) -> Tuple[bool, List[str], Dict]:
        """
        Validate a single product
        
        Returns:
            (is_valid, issues_list, cleaned_product)
        """
        issues = []
        cleaned = product.copy()
        
        # 1. Check required fields
        required_fields = ["product_id", "name", "price", "stock"]
        for field in required_fields:
            if field not in product:
                issues.append(f"missing_field_{field}")
        
        if issues:  # Can't continue without required fields
            self._track_issues(issues)
            return False, issues, None
        
        # 2. Validate product_id
        if not product["product_id"] or product["product_id"].strip() == "":
            issues.append("empty_product_id")
        
        # 3. Check for duplicates
        if product["product_id"] in self.seen_product_ids:
            issues.append("duplicate_product_id")
            self.validation_stats["duplicates"] += 1
        else:
            self.seen_product_ids.add(product["product_id"])
        
        # 4. Validate and clean name
        if product["name"] is None or str(product["name"]).strip() == "":
            issues.append("missing_or_empty_name")
        else:
            # Clean whitespace
            cleaned["name"] = str(product["name"]).strip()
        
        # 5. Validate and clean price
        price_valid, price_cleaned, price_issues = self._validate_price(product["price"])
        if not price_valid:
            issues.extend(price_issues)
        else:
            cleaned["price"] = price_cleaned
        
        # 6. Validate and clean stock
        stock_valid, stock_cleaned, stock_issues = self._validate_stock(product["stock"])
        if not stock_valid:
            issues.extend(stock_issues)
        else:
            cleaned["stock"] = stock_cleaned
        
        # 7. Validate category (if present)
        if "category" in product:
            cleaned["category"] = str(product["category"]).strip()
        
        # Track issues
        self._track_issues(issues)
        
        # Valid if no issues
        is_valid = len(issues) == 0
        
        return is_valid, issues, cleaned if is_valid else None
    
    def _validate_price(self, price) -> Tuple[bool, float, List[str]]:
        """
        Validate and clean price field
        
        Returns:
            (is_valid, cleaned_value, issues)
        """
        issues = []
        
        # Handle None
        if price is None:
            return False, None, ["price_is_null"]
        
        # If already a number
        if isinstance(price, (int, float)):
            if price <= 0:
                return False, None, ["price_not_positive"]
            return True, float(price), []
        
        # If string, try to clean and parse
        if isinstance(price, str):
            price_str = price.strip()
            
            # Check for non-numeric strings
            if price_str.upper() in ["CALL", "N/A", "TBD", ""]:
                return False, None, ["price_invalid_string"]
            
            # Remove currency symbols and commas
            cleaned_price = re.sub(r'[$,£€]', '', price_str)
            
            try:
                price_float = float(cleaned_price)
                if price_float <= 0:
                    return False, None, ["price_not_positive"]
                return True, price_float, []
            except ValueError:
                return False, None, ["price_cannot_convert_to_number"]
        
        return False, None, ["price_unexpected_type"]
    
    def _validate_stock(self, stock) -> Tuple[bool, int, List[str]]:
        """
        Validate and clean stock field
        
        Returns:
            (is_valid, cleaned_value, issues)
        """
        issues = []
        
        # Handle None
        if stock is None:
            return False, None, ["stock_is_null"]
        
        # If already a number
        if isinstance(stock, int):
            if stock < 0:
                return False, None, ["stock_negative"]
            return True, stock, []
        
        # If float, convert to int
        if isinstance(stock, float):
            stock_int = int(stock)
            if stock_int < 0:
                return False, None, ["stock_negative"]
            return True, stock_int, []
        
        # If string, try to parse
        if isinstance(stock, str):
            try:
                stock_int = int(stock.strip())
                if stock_int < 0:
                    return False, None, ["stock_negative"]
                return True, stock_int, []
            except ValueError:
                return False, None, ["stock_cannot_convert_to_integer"]
        
        return False, None, ["stock_unexpected_type"]
    
    def _track_issues(self, issues: List[str]):
        """Track issue types for reporting"""
        for issue in issues:
            if issue not in self.validation_stats["issues_breakdown"]:
                self.validation_stats["issues_breakdown"][issue] = 0
            self.validation_stats["issues_breakdown"][issue] += 1
    
    def validate_batch(self, products: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Validate a batch of products
        
        Returns:
            (clean_products, quarantined_products)
        """
        clean_products = []
        quarantined_products = []
        
        self.validation_stats["total"] = len(products)
        
        for product in products:
            is_valid, issues, cleaned = self.validate_product(product)
            
            if is_valid:
                self.validation_stats["valid"] += 1
                clean_products.append(cleaned)
            else:
                self.validation_stats["invalid"] += 1
                quarantined_products.append({
                    "raw_data": product,
                    "issues": issues,
                    "quarantined_at": datetime.now().isoformat()
                })
        
        return clean_products, quarantined_products
    
    def get_stats(self) -> Dict:
        """Get validation statistics"""
        return self.validation_stats
    
    def print_report(self):
        """Print validation report"""
        print("\n" + "="*60)
        print("BATCH VALIDATION REPORT")
        print("="*60)
        print(f"Total products: {self.validation_stats['total']}")
        print(f"✅ Valid: {self.validation_stats['valid']}")
        print(f"❌ Invalid: {self.validation_stats['invalid']}")
        print(f"🔄 Duplicates: {self.validation_stats['duplicates']}")
        
        if self.validation_stats["issues_breakdown"]:
            print("\nIssue Breakdown:")
            for issue, count in sorted(
                self.validation_stats["issues_breakdown"].items(),
                key=lambda x: x[1],
                reverse=True
            ):
                print(f"  • {issue}: {count}")
        
        if self.validation_stats["total"] > 0:
            quality_pct = (self.validation_stats["valid"] / self.validation_stats["total"]) * 100
            print(f"\n📈 Data Quality: {quality_pct:.1f}%")
        
        print("="*60)


if __name__ == "__main__":
    # Test the validator
    from scraper import ProductScraper
    
    print("Testing validator with sample data...\n")
    
    scraper = ProductScraper()
    products = scraper.scrape_source("test", num_products=20)
    
    validator = ProductValidator()
    clean, quarantine = validator.validate_batch(products)
    
    validator.print_report()
    
    print(f"\n✅ Clean products: {len(clean)}")
    print(f"❌ Quarantined: {len(quarantine)}")
    
    if quarantine:
        print("\nSample quarantined record:")
        print(json.dumps(quarantine[0], indent=2))