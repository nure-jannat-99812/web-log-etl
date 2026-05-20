import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, Generator, Optional
import json
import yaml

class LogGenerator:
    def __init__(self, config_path: Optional[str] = None):
        self.config = self._load_config(config_path)
        self._setup_data()
    
    def _load_config(self, config_path):
        if config_path:
            try:
                with open(config_path, 'r') as f:
                    return yaml.safe_load(f)
            except:
                pass
        return {}
    
    def _setup_data(self):
        # Common endpoints
        self.endpoints = [
            "/api/users", "/api/products", "/api/orders", "/home",
            "/products", "/search", "/login", "/logout", "/cart", "/checkout"
        ]
        
        # HTTP methods
        self.methods = ["GET", "POST", "PUT", "DELETE"]
        self.method_weights = [0.70, 0.20, 0.05, 0.05]
        
        # Status codes
        self.status_codes = [200, 201, 400, 401, 404, 500, 302]
        self.status_weights = [0.85, 0.03, 0.02, 0.01, 0.05, 0.02, 0.02]
        
        # User agents
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1) Mobile/15E148",
            "Mozilla/5.0 (Linux; Android 13) Chrome/120.0 Mobile",
            "curl/7.68.0",
            "Mozilla/5.0 (compatible; Googlebot/2.1)"
        ]
        
        # IP ranges (prefix, country)
        self.ip_ranges = [
            ("192.168.1.", "US"), ("10.0.0.", "GB"), ("172.16.0.", "DE"),
            ("192.168.2.", "FR"), ("10.1.0.", "JP"), ("192.168.3.", "CA")
        ]
    
    def _get_traffic_multiplier(self, hour: int) -> float:
        peak_hours = self.config.get('peak_hours', [9,10,11,12,13,14,15,16,17])
        if hour in peak_hours:
            return self.config.get('peak_multiplier', 3.0)
        elif hour < 6 or hour > 22:
            return self.config.get('night_multiplier', 0.1)
        return 1.0
    
    def _generate_ip(self):
        prefix, country = random.choice(self.ip_ranges)
        last_octet = random.randint(1, 254)
        return f"{prefix}{last_octet}", country
    
    def _generate_endpoint(self):
        endpoint = random.choice(self.endpoints)
        # Add query params occasionally
        if random.random() < 0.2:
            params = []
            if random.random() < 0.5:
                params.append(f"page={random.randint(1,10)}")
            if random.random() < 0.3:
                params.append(f"limit={random.choice([10,25,50])}")
            if params:
                endpoint += "?" + "&".join(params)
        return endpoint
    
    def generate_log(self, timestamp: datetime) -> Dict:
        """Generate a single log entry"""
        ip, country = self._generate_ip()
        method = random.choices(self.methods, weights=self.method_weights)[0]
        endpoint = self._generate_endpoint()
        status = random.choices(self.status_codes, weights=self.status_weights)[0]
        ua = random.choice(self.user_agents)
        
        # Response size based on status and endpoint
        if status >= 400:
            size = random.randint(100, 2000)
        elif "static" in endpoint:
            size = random.randint(5000, 500000)
        else:
            size = random.randint(500, 50000)
        
        return {
            "log_id": str(uuid.uuid4()),
            "timestamp": timestamp.isoformat(),
            "ip": ip,
            "country": country,
            "method": method,
            "endpoint": endpoint,
            "status_code": status,
            "response_size": size,
            "user_agent": ua,
            "referer": random.choice(["-", "https://google.com", "https://github.com"]),
            "is_bot": "bot" in ua.lower()
        }
    
    def generate_logs_for_day(self, date: datetime, logs_per_hour: int = 1000) -> Generator[Dict, None, None]:
        """Generate logs for a full day"""
        start = datetime(date.year, date.month, date.day, 0, 0, 0)
        
        for hour in range(24):
            hour_start = start + timedelta(hours=hour)
            multiplier = self._get_traffic_multiplier(hour)
            logs_this_hour = int(logs_per_hour * multiplier)
            
            # Distribute across minutes
            for minute in range(60):
                minute_start = hour_start + timedelta(minutes=minute)
                logs_this_minute = max(1, logs_this_hour // 60 + random.randint(-2, 2))
                
                for _ in range(logs_this_minute):
                    ms = random.randint(0, 999)
                    timestamp = minute_start + timedelta(milliseconds=ms)
                    yield self.generate_log(timestamp)
