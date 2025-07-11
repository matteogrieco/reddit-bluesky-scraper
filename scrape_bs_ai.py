
from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message, CAR, IdResolver, DidInMemoryCache
import json
import time
import argparse
from datetime import datetime
import multiprocessing
import sys
import signal
import re

# ===============================================
# ✅ Mot-clés IA professionnels (hashtags + mots)
# ===============================================

IA_KEYWORDS = [
    # Modèles et outils connus
    "gpt", "chatgpt", "gpt-2", "gpt-3", "gpt-4", "gpt-4o", "openai", "dall-e", "copilot",
    "bard", "gemini", "claude", "anthropic", "mistral", "llama", "llama2", "llama 2",
    "meta ai", "meta-ai", "midjourney", "stability ai", "perplexity", "replika", "pi ai",

    # Concepts IA en anglais
    "ai", "a.i.", "artificial intelligence", "machine learning", "deep learning",
    "neural network", "neural networks", "language model", "large language model", "llm",
    "superintelligence", "strong ai", "weak ai", "general ai", "narrow ai", "agi",
    "autonomous agent", "ai-powered", "agi-powered", "genai", "gen-ai", "ai system",

    # Concepts IA en français
    "ia", "intelligence artificielle", "apprentissage automatique", "apprentissage profond",
    "réseau neuronal", "modèle de langage", "modèle de langage étendu", "agent autonome",
    "ia générative", "outil ia", "système automatisé", "assistant intelligent",

    # Concepts IA en espagnol, allemand, italien
    "inteligencia artificial", "aprendizaje automático", "modelo de lenguaje",
    "intelligenza artificiale", "apprendimento automatico", "modello linguistico",
    "kunstliche intelligenz", "neuronales netz", "sprachmodell"
]

# 🔎 Compile regex IA
ia_regex = re.compile(r"\b(" + "|".join(re.escape(term) for term in IA_KEYWORDS) + r")\b", re.IGNORECASE)

# ===============================================
# 🔧 Worker et traitement
# ===============================================

def worker_process(queue, output_file, verbose, post_count, lock, stop_event):
    resolver = IdResolver(cache=DidInMemoryCache())
    while not stop_event.is_set():
        try:
            message = queue.get(timeout=1)
            process_message(message, resolver, output_file, verbose, post_count, lock)
        except multiprocessing.queues.Empty:
            continue
        except Exception as e:
            print(f"Worker error: {e}")

def client_process(queue, stop_event):
    client = FirehoseSubscribeReposClient()
    def message_handler(message):
        if stop_event.is_set():
            client.stop()
            return
        queue.put(message)

    try:
        client.start(message_handler)
    except Exception as e:
        if not stop_event.is_set():
            print(f"Client process error: {e}")

def process_message(message, resolver, output_file, verbose, post_count, lock):
    try:
        commit = parse_subscribe_repos_message(message)
        if not hasattr(commit, 'ops'):
            return

        for op in commit.ops:
            if op.action == 'create' and op.path.startswith('app.bsky.feed.post/'):
                _process_post(commit, op, resolver, output_file, verbose, post_count, lock)
    except Exception as e:
        print(f"Error processing message: {e}")

def _process_post(commit, op, resolver, output_file, verbose, post_count, lock):
    try:
        author_handle = _resolve_author_handle(commit.repo, resolver)
        car = CAR.from_bytes(commit.blocks)
        for record in car.blocks.values():
            if isinstance(record, dict) and record.get('$type') == 'app.bsky.feed.post':
                post_data = _extract_post_data(record, commit.repo, op.path, author_handle)
                _save_post_data(post_data, output_file, verbose, post_count, lock)
    except Exception as e:
        print(f"Error processing record: {e}")

def _resolve_author_handle(repo, resolver):
    try:
        resolved_info = resolver.did.resolve(repo)
        return resolved_info.also_known_as[0].split('at://')[1] if resolved_info.also_known_as else repo
    except Exception as e:
        print(f"Could not resolve handle for {repo}: {e}")
        return repo

def _extract_post_data(record, repo, path, author_handle):
    has_images = _check_for_images(record)
    reply_to = _get_reply_to(record)
    return {
        'text': record.get('text', ''),
        'created_at': record.get('createdAt', ''),
        'author': author_handle,
        'uri': f'at://{repo}/{path}',
        'has_images': has_images,
        'reply_to': reply_to
    }

def _check_for_images(record):
    embed = record.get('embed', {})
    return (
        embed.get('$type') == 'app.bsky.embed.images' or
        (embed.get('$type') == 'app.bsky.embed.external' and 'thumb' in embed)
    )

def _get_reply_to(record):
    reply_ref = record.get('reply', {})
    return reply_ref.get('parent', {}).get('uri')

def _save_post_data(post_data, output_file, verbose, post_count, lock):
    text = post_data.get("text", "").lower()
    if not ia_regex.search(text):
        return

    with lock:
        with open(output_file, 'a', encoding='utf-8') as f:
            json.dump(post_data, f, ensure_ascii=False)
            f.write('\n')

    with post_count.get_lock():
        post_count.value += 1

    if verbose:
        print(f"✅ @{post_data['author']}: {post_data['text'][:80]}...")

# ===============================================
# ▶️ Lancement
# ===============================================

class FirehoseScraper:
    def __init__(self, output_file="ai_posts.jsonl", verbose=False, num_workers=4):
        self.output_file = output_file
        self.post_count = multiprocessing.Value('i', 0)
        self.start_time = None
        self.cache = DidInMemoryCache()
        self.resolver = IdResolver(cache=self.cache)
        self.verbose = verbose
        self.queue = multiprocessing.Queue()
        self.num_workers = num_workers
        self.workers = []
        self.stop_event = multiprocessing.Event()
        self.lock = multiprocessing.Lock()
        self.client_proc = None

    def start_collection(self, duration_seconds=None, post_limit=None):
        print(f"Starting collection{f' for {post_limit} posts' if post_limit else ''}...")
        self.start_time = time.time()
        end_time = self.start_time + duration_seconds if duration_seconds else None

        for _ in range(self.num_workers):
            p = multiprocessing.Process(
                target=worker_process,
                args=(self.queue, self.output_file, self.verbose, self.post_count, self.lock, self.stop_event)
            )
            p.start()
            self.workers.append(p)

        def signal_handler(sig, frame):
            print("\nCollection stopped by user.")
            self._stop_collection()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)

        while True:
            self.client_proc = multiprocessing.Process(
                target=client_process,
                args=(self.queue, self.stop_event)
            )
            self.client_proc.start()

            try:
                while True:
                    if self.stop_event.is_set():
                        break
                    if duration_seconds and time.time() > end_time:
                        print("\nTime limit reached.")
                        self._stop_collection()
                        break
                    elif post_limit and self.post_count.value >= post_limit:
                        print("\nPost limit reached.")
                        self._stop_collection()
                        break
                    if not self.client_proc.is_alive():
                        if not self.stop_event.is_set():
                            print("\nClient process exited unexpectedly.")
                            self._stop_collection()
                            break
                        else:
                            break
                    time.sleep(1)
                else:
                    break
                if self.stop_event.is_set():
                    break
            except KeyboardInterrupt:
                print("\nCollection interrupted by user.")
                self._stop_collection()
                break
            except Exception as e:
                error_details = f"{type(e).__name__}: {str(e)}" if str(e) else f"{type(e).__name__}"
                print(f"\nConnection error: {error_details}")

        self._stop_collection()

    def _stop_collection(self):
        if not self.stop_event.is_set():
            self.stop_event.set()

        if self.client_proc and self.client_proc.is_alive():
            self.client_proc.terminate()
            self.client_proc.join()

        for p in self.workers:
            if p.is_alive():
                p.terminate()
            p.join()

        elapsed = time.time() - self.start_time if self.start_time else 0
        rate = self.post_count.value / elapsed if elapsed > 0 else 0
        print("\nCollection complete!")
        print(f"Collected {self.post_count.value} posts in {elapsed:.2f} seconds")
        print(f"Average rate: {rate:.1f} posts/sec")
        print(f"Output saved to: {self.output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Collect AI-related posts professionally from the Bluesky firehose')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-t', '--time', type=int, help='Collection duration in seconds')
    group.add_argument('-n', '--number', type=int, help='Number of posts to collect')
    parser.add_argument('-o', '--output', type=str,
                        default=f"ai_filtered_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl",
                        help='Output file path')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Print each post as it is collected')
    parser.add_argument('-w', '--workers', type=int, default=4,
                        help='Number of worker processes')

    args = parser.parse_args()
    archiver = FirehoseScraper(output_file=args.output, verbose=args.verbose, num_workers=args.workers)
    archiver.start_collection(duration_seconds=args.time, post_limit=args.number)
