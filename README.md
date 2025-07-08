# reddit-bluesky-scraper
Asynchronous data scraper for Reddit and Bluesky using keyword filters.

# Reddit & Bluesky Scrapers

This repository contains two asynchronous scrapers:

- **Reddit scraper** using PRAW to collect posts from selected subreddits based on keyword filtering.
- **Bluesky firehose listener** using the `atproto` client to detect and save posts containing specific keywords in real time.

## Features

- Keyword-based filtering (case-insensitive)
- Incremental saving (avoid duplicates)
- JSONL output (optionally gzip-compressed for Bluesky)
- Session persistence for Bluesky

## Requirements

- Python 3.9+
- Reddit API credentials (client ID, secret, user-agent)
- Bluesky account credentials (via environment variables)

