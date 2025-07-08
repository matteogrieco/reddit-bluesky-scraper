import praw
import json
import os
import time

# --- CONFIG --- #
CLIENT_ID = ''
CLIENT_SECRET = ''
USER_AGENT = '' 


SUBREDDITS = [
SUBREDDITS.jsonl
]


KEYWORDS = [
    KEYWORDS.txt
]

# Nombre de posts à collecter
TARGET_POSTS = 100000


OUTPUT_FILE = 'reddit_posts_economia.jsonl'

# --- SCRAP MATTEO --- #

def connect_to_reddit():
    
    try:
        reddit = praw.Reddit(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            user_agent=USER_AGENT
        )
        print("Connexion à Reddit réussie.")
        return reddit
    except Exception as e:
        print(f"Erreur de connexion à Reddit : {e}")
        return None

def post_matches_keywords(post, keywords):
    """Vérifie si le titre ou le texte du post contient des mots-clés pertinents.
    La recherche est insensible à la casse.
    """
    text_to_search = f"{post.title} {post.selftext if hasattr(post, 'selftext') else ''}".lower()
    for keyword in keywords:
        if keyword.lower() in text_to_search:
            return True
    return False

def collect_reddit_posts(reddit_instance, subreddits, keywords, target_count, output_file):
    """Collecte les posts Reddit et les sauvegarde dans un fichier JSONL.
    Gère les limites de l'API et la reprise en cas d'interruption.
    """
    collected_count = 0
    processed_ids = set() # Pour éviter les doublons si le script est relancé

    # Charger les posts déjà collectés si le fichier existe
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    post_data = json.loads(line)
                    processed_ids.add(post_data['id'])
                    collected_count += 1
                except json.JSONDecodeError:
                    continue # Ignorer les lignes mal formées
        print(f"Reprise de la collecte. {collected_count} posts déjà collectés.")

    with open(output_file, 'a', encoding='utf-8') as f_out:
        for subreddit_name in subreddits:
            if collected_count >= target_count:
                break

            print(f"\nCollecte depuis r/{subreddit_name}...")
            subreddit = reddit_instance.subreddit(subreddit_name)

            
           
            for post in subreddit.new(limit=None): 
                if collected_count >= target_count:
                    break
                if post.id in processed_ids:
                    continue 

                
                if post_matches_keywords(post, keywords):
                    post_data = {
                        'id': post.id,
                        'subreddit': post.subreddit.display_name,
                        'title': post.title,
                        'selftext': post.selftext if hasattr(post, 'selftext') else '',
                        'url': post.url,
                        'author': str(post.author), 
                        'score': post.score,
                        'num_comments': post.num_comments,
                        'created_utc': post.created_utc,
                        'permalink': post.permalink,
                        'domain': post.domain
                    }
                    f_out.write(json.dumps(post_data) + '\n')
                    collected_count += 1
                    processed_ids.add(post.id)

                    if collected_count % 100 == 0: 
                        print(f"Progression: {collected_count}/{target_count} posts collectés.")

               
                time.sleep(1) 

    print(f"\nCollecte terminée. Total de {collected_count} posts collectés et sauvegardés dans {output_file}.")

# --- EXEC --- #

if __name__ == "__main__":
    reddit = connect_to_reddit()
    if reddit:
        collect_reddit_posts(reddit, SUBREDDITS, KEYWORDS, TARGET_POSTS, OUTPUT_FILE)
    else:
        print("Impossible de se connecter à Reddit. Veuillez vérifier vos identifiants.")


