"""Тест NewsAPI"""
import asyncio
import aiohttp
import json
import sys
import io

# Исправление кодировки для Windows
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

async def test_newsapi():
    api_key = '04e5d76c7e7648cbbcfb494521f4598a'
    base_url = 'https://newsapi.org/v2/top-headlines'
    
    test_cases = [
        {'country': 'ru', 'category': 'general'},
        {'country': 'us', 'category': 'general'},
        {'country': 'ru', 'category': 'technology'},
        {'country': 'us', 'category': 'technology'},
        {'country': 'gb', 'category': 'general'},
    ]
    
    async with aiohttp.ClientSession() as session:
        for i, params in enumerate(test_cases, 1):
            params_copy = params.copy()
            params_copy['apiKey'] = api_key
            params_copy['pageSize'] = 5
            
            print(f"\n--- Test {i}: {params} ---")
            
            async with session.get(base_url, params=params_copy) as resp:
                data = await resp.json()
                
                if data.get('status') == 'ok':
                    articles = data.get('articles', [])
                    print(f"Articles: {len(articles)}")
                    for a in articles[:2]:
                        title = a.get('title', 'No title')[:60]
                        print(f"  - {title}")
                else:
                    print(f"Error: {data.get('message')}")

if __name__ == "__main__":
    asyncio.run(test_newsapi())
