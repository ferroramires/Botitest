import tweepy

consumer_key = ''
consumer_secret = ''
token = ''

authentication = tweepy.OAuthHandler(consumer_key, consumer_secret)
redirect_url = authentication.get_authorization_url()
api = tweepy.API(authentication)


lang = 'pt-BR'
result_type = 'recent'
max_tweets = 50
location = 'Brazil'
top_tweets = True

api = tweepy.API(authentication)
results = api.search(q='Boticário')

#for tweet in resultados:
#     print(f'Usuário: {tweet.user} - Tweet: {tweet.text}')


def tweets(search_words, numTweets, numRuns):

    tweets = tweepy.Cursor(api.search, q=search_words, lang="pt").items(numTweets)

    tweet_list = [tweet for tweet in tweets]

    for tweet in tweet_list:
        text = tweet.text
        user = tweet.user.screen_name
        print(user + "- "+text)

        values = (user, text)

        query = """INSERT INTO silver.tweeter_data (user, text) VALUES (%s, %s)"""

    # Commit the transaction

search_words = "Boticario OR SOLAR"

tweets(search_words, numTweets=50,numRuns=6)