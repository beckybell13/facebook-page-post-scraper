import json
import datetime
import csv
import time
import re
import random
try:
    from urllib.request import urlopen, Request
except ImportError:
    from urllib2 import urlopen, Request

page_id = "beaverconfessions"
file_id = "beaverconfessions"
access_token = "USER_TOKEN_HERE"

# input date formatted as YYYY-MM-DD
since_date = "2012-01-01"
until_date = "2017-11-02"

REPLACE_TAGGED_NAMES = False


def request_until_succeed(url):
    req = Request(url)
    success = False
    while success is False:
        try:
            response = urlopen(req)
            if response.getcode() == 200:
                success = True
        except Exception as e:
            print(e)
            time.sleep(5)

            print("Error for URL {}: {}".format(url, datetime.datetime.now()))
            print("Retrying.")

    return response.read()


# Needed to write tricky unicode correctly to csv
def unicode_decode(text):
    try:
        # remove newlines
        text = text.encode('utf-8').decode()
        return text.replace("\n", "")
    except UnicodeDecodeError:
        return text.encode('utf-8')


def getFacebookPageFeedUrl(base_url):

    # Construct the URL string; see http://stackoverflow.com/a/37239851 for
    #   reactions parameters
    fields = "&fields=message,link,created_time,id," + \
        "tags,place,reactions.limit(0).summary(true)"

    return base_url + fields


def getReactionsForStatuses(base_url):

    reaction_types = ['like', 'love', 'wow', 'haha', 'sad', 'angry']
    reactions_dict = {}   # dict of {status_id: tuple<6>}

    for reaction_type in reaction_types:
        fields = "&fields=reactions.type({}).limit(0).summary(total_count)".format(
            reaction_type.upper())

        url = base_url + fields

        data = json.loads(request_until_succeed(url))['data']

        data_processed = set()  # set() removes rare duplicates in statuses
        for status in data:
            id = status['id']
            count = status['reactions']['summary']['total_count']
            data_processed.add((id, count))

        for id, count in data_processed:
            if id in reactions_dict:
                reactions_dict[id] = reactions_dict[id] + (count,)
            else:
                reactions_dict[id] = (count,)

    return reactions_dict


"""
Filters messages based on the following criteria:
    1) Remove tagged names (might have to ignore for statuses...also might not be a problem)
    2) Remove links
    3) Remove special characters and emojis
"""
def filterMessage(status_message, link, tags):
    # Remove any new lines or carriage returns within message
    status_message = re.sub(r'[\r\n]+', ' ', status_message)

    if link != '':
        status_message = status_message.replace(link, '')

    # Use regex to remove links, because FB API is not helpful here :/
    status_message = re.sub(r'https?:\/\/[^ ]*', '', status_message)

    if tags != []:
        # print 'tag(s) found in status:'
        # print(status_message)
        # print(tags)
        for t in tags:
            # print status_message # tags in confession posts are very rare, curious to see what they are
            # print t # Also: this might just return the user id...which doesn't help...
            if REPLACE_TAGGED_NAMES:
                status_message = status_message.replace(t, '')
                status_message = re.sub(r"(^ *[',!?.:;] *)|( +[',.:;]s?)", "", status_message)
            else:
                status_message = status_message.replace(t, ' CUSTOM_NAME ')
        #print(status_message)

    # Remove any special unicode characters
    status_message = re.sub(r'(?!\\u2019|\\u2018)\\u[0-9A-Fa-f]{4,8}', '', status_message.encode('unicode_escape'), flags=re.IGNORECASE)
    # Replace fancy apostrophe/quote characters
    status_message = re.sub(r'(\\u2019|\\u2018)', "'", status_message.encode('unicode_escape'), flags=re.IGNORECASE)
    #Remove lingering back-slashes...
    status_message = re.sub(r"\\", "", status_message)

    status_message = ' '.join(status_message.split())

    return status_message


def processFacebookPageFeedStatus(status):

    # The status is now a Python dictionary, so for top-level items,
    # we can simply call the key.

    # Additionally, some items may not always exist,
    # so must check for existence first

    status_id = status['id']

    status_message = '' if 'message' not in status else \
        status['message']
    status_link = '' if 'link' not in status else \
        status['link']
    tags = [] if 'tags' not in status else \
        status['tags']

    # Pre-process message content
    status_message = unicode_decode(filterMessage(status_message, status_link, tags))

    # Time needs special care since a) it's in UTC and
    # b) it's not easy to use in statistical programs.

    status_published = datetime.datetime.strptime(
        status['created_time'], '%Y-%m-%dT%H:%M:%S+0000')
    status_published = status_published + \
        datetime.timedelta(hours=-5)  # EST
    status_published = status_published.strftime(
        '%Y-%m-%d %H:%M:%S')  # best time format for spreadsheet programs

    # Nested items require chaining dictionary keys.
    num_reactions = 0 if 'reactions' not in status else \
        status['reactions']['summary']['total_count']

    return (status_id, status_published, status_message, num_reactions)


def scrapeFacebookPageFeedStatus(page_id, access_token, since_date, until_date):
    with open('data\{}_facebook_statuses.csv'.format(page_id), 'wb') as file:
        w = csv.writer(file)
        w.writerow(["status_id", "status_published", "status_message", "num_reactions",
                    "num_likes", "num_loves", "num_wows", "num_hahas", "num_sads",
                    "num_angrys", "num_special"])

        has_next_page = True
        num_processed = 0
        scrape_starttime = datetime.datetime.now()
        after = ''
        base = "https://graph.facebook.com/v2.9"
        node = "/{}/posts".format(page_id)
        parameters = "/?limit={}&access_token={}".format(100, access_token)
        since = "&since={}".format(since_date) if since_date \
            is not '' else ''
        until = "&until={}".format(until_date) if until_date \
            is not '' else ''

        print("Scraping {} Facebook Page: {}\n".format(page_id, scrape_starttime))

        while has_next_page:
            after = '' if after is '' else "&after={}".format(after)
            base_url = base + node + parameters + after + since + until

            url = getFacebookPageFeedUrl(base_url)
            statuses = json.loads(request_until_succeed(url))
            reactions = getReactionsForStatuses(base_url)

            for status in statuses['data']:

                # Ensure it is a status with the expected metadata
                if 'reactions' in status:
                    status_data = processFacebookPageFeedStatus(status)
                    reactions_data = reactions[status_data[0]]

                    # calculate thankful/pride through algebra
                    num_special = status_data[-1] - sum(reactions_data)

                    # Enforce length criteria
                    status_message = status_data[-2]
                    if len(status_message) >= 50 and len(status_message) <= 280:
                        # Enforce actual confession post by checking for beginning '#''
                        if status_message[0] == '#':
                            w.writerow(status_data + reactions_data + (num_special,))

                num_processed += 1
                if num_processed % 500 == 0:
                    print("{} Statuses Processed: {}".format
                          (num_processed, datetime.datetime.now()))

            # if there is no next page, we're done.
            if 'paging' in statuses:
                after = statuses['paging']['cursors']['after']
            else:
                has_next_page = False

        print("\nDone!\n{} Statuses Processed in {}".format(
              num_processed, datetime.datetime.now() - scrape_starttime))


if __name__ == '__main__':
    scrapeFacebookPageFeedStatus(page_id, access_token, since_date, until_date)
