# reddit-bluesky-scraper
This repository contains two keyword-based scrapers for Reddit and Bluesky.

# Reddit & Bluesky Scrapers

This repository contains two scrapers:

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

