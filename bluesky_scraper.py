import os
import sys
import datetime
import time
import gzip
import logging.handlers
from atproto_client import Client, SessionEvent
from atproto.exceptions import RequestException, BadRequestError
from atproto_client.models.app.bsky.feed.post import Post as AtprotoPost

log = logging.getLogger("bot")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

USERNAME = os.environ.get("")
PASSWORD = os.environ.get("")


KEYWORDS = []
with open("/home/ubuntu/upload/keywords.txt", "r") as f:
    for line in f:
        keyword = line.strip().lower()
        if keyword:
            KEYWORDS.append(keyword)




def get_session():
    try:
        with open("session.txt") as f:
            return f.read()
    except FileNotFoundError:
        return None


def save_session(session_string):
    with open("session.txt", "w") as f:
        f.write(session_string)


def on_session_change(event, session):
    print("Session changed:", event, repr(session))
    if event in (SessionEvent.CREATE, SessionEvent.REFRESH):
        print("Saving changed session")
        save_session(session.export())


def init_client(USERNAME, PASSWORD):
    client = Client()
    client.on_session_change(on_session_change)

    session_string = get_session()
    if session_string:
        print("Reusing session")
        client.login(session_string=session_string)
    else:
        print("Creating new session")
        client.login(USERNAME, PASSWORD)

    return client



def _save_post(post, file_id):
    
    if not os.path.exists(f'data/economia'):
        os.makedirs(f'data/economia')

    with gzip.open(f'data/economia/posts-{file_id}.jsonl.gz', 'a') as f:
        row = f"{post.json()}\n"
        f.write(row.encode('utf8'))
    print(f'{datetime.datetime.now()} SAVED post to data/economia/posts-{file_id}.jsonl.gz')


def process_firehose_message(message):
    if isinstance(message, AtprotoPost):
        text = message.text.lower()
        for keyword in KEYWORDS:
            if keyword in text:
                print(f"Found keyword '{keyword}' in post: {message.text}")
                _save_post(message, datetime.datetime.now().strftime("%Y%m%d%H"))
                break


if __name__ == '__main__':
    client = init_client(USERNAME, PASSWORD)
    print("Starting firehose subscription...")
    client.start_listening_firehose(process_firehose_message)


