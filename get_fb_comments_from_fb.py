# coding=utf-8

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
access_token = "EAACEdEose0cBAEVvZAdFoVDZAQqLxmtcdMuIbIhKVT29DBhwZB57PJso8bXfXSbAE4DNcXM5ZB5WVgU87sb2ReoEDJxGXFNllI3ZAceLShZBgLEwUPdcM3KsVGGvS182cnZAsOUW2mCT565hOS2CjhkpGLvPqABjADSZC1ykXbuMRNy1HE8cO44JjBsEu1r52vVHNdrAWnAP3wZDZD"

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
        return text.encode('utf-8').decode()
    except UnicodeDecodeError:
        return text.encode('utf-8')


def getFacebookCommentFeedUrl(base_url):

    # Construct the URL string
    fields = "&fields=id,message,created_time,attachment,message_tags" + \
        ",reactions.limit(0).summary(true)"
    url = base_url + fields

    return url


def getReactionsForComments(base_url):

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
    1) Remove tagged names
    2) Remove links
    3) Remove special characters and emojis
"""
def filterMessage(status_message, tags, attachment):
    # Remove any new lines or carriage returns within message
    status_message = re.sub(r'[\r\n]+', ' ', status_message)

    # Remove links
    if attachment != {}:
        # Use regex to remove links, because FB API is not helpful here :/
        status_message = re.sub(r'https?:\/\/[^ ]*', '', status_message)

    # Remove user tags
    if tags != []:
        for t in tags:
            # include page or group tags, but not users
            if t['type'] == 'user':
                if REPLACE_TAGGED_NAMES:
                    status_message = status_message.replace(t['name'], '')
                    # Remove floating punctuation at beginning of string
                    # Remove floating punctuation or 's anywhere else
                    # Floating punctuation defined as certain punctuation marks preceded by at least one space
                    status_message = re.sub(r"(^ *[',!?.:;] *)|( +[',.:;]s?)", "", status_message)
                else:
                    status_message = status_message.replace(t['name'], ' CUSTOM_NAME ')

    # Remove any special unicode characters
    status_message = re.sub(r'(?!\\u2019|\\u2018)\\u[0-9A-Fa-f]{4,8}', '', status_message.encode('unicode_escape'), flags=re.IGNORECASE)
    # Replace fancy apostrophe/quote characters
    status_message = re.sub(r'(\\u2019|\\u2018)', "'", status_message.encode('unicode_escape'), flags=re.IGNORECASE)
    #Remove lingering back-slashes...
    status_message = re.sub(r"\\", "", status_message)

    status_message = ' '.join(status_message.split())
    
    # Add fake confession number
    fake_confession_num = random.randint(1,99999)
    status_message = '#{} {}'.format(fake_confession_num, status_message)

    return status_message


def processFacebookComment(comment, status_id, parent_id=''):

    # The status is now a Python dictionary, so for top-level items,
    # we can simply call the key.

    # Additionally, some items may not always exist,
    # so must check for existence first

    comment_id = comment['id']

    comment_message = '' if 'message' not in comment or comment['message'] \
        is '' else comment['message']

    num_reactions = 0 if 'reactions' not in comment else \
        comment['reactions']['summary']['total_count']

    comment_tags = [] if 'message_tags' not in comment else \
        comment['message_tags']

    comment_attachment = {} if 'attachment' not in comment else \
        comment['attachment']

    """
    Process message here
    """
    comment_message = unicode_decode(filterMessage(comment_message, comment_tags, comment_attachment))

    # Time needs special care since a) it's in UTC and
    # b) it's not easy to use in statistical programs.

    comment_published = datetime.datetime.strptime(
        comment['created_time'], '%Y-%m-%dT%H:%M:%S+0000')
    comment_published = comment_published + datetime.timedelta(hours=-5)  # EST
    comment_published = comment_published.strftime(
        '%Y-%m-%d %H:%M:%S')  # best time format for spreadsheet programs

    # Return a tuple of all processed data
    return (comment_id, status_id, parent_id, comment_published, comment_message, num_reactions)


def scrapeFacebookPageFeedComments(page_id, access_token):
    with open('data\{}_facebook_comments.csv'.format(file_id), 'wb') as file:
        w = csv.writer(file)
        w.writerow(["comment_id", "status_id", "parent_id", "comment_published",
                    "comment_message", "num_reactions", "num_likes", "num_loves",
                    "num_wows", "num_hahas", "num_sads", "num_angrys", "num_special"])

        num_processed = 0
        scrape_starttime = datetime.datetime.now()
        after = ''
        base = "https://graph.facebook.com/v2.9"
        parameters = "/?limit={}&access_token={}".format(
            100, access_token)

        print("Scraping {} Comments From Posts: {}\n".format(
            file_id, scrape_starttime))

        # NOTE: must have downloaded statuses beforehand in order to get comments
        with open('data\{}_facebook_statuses.csv'.format(file_id), 'r') as csvfile:
            reader = csv.DictReader(csvfile)

            # Uncomment below line to scrape comments for a specific status_id
            # reader = [dict(status_id='5550296508_10154352768246509')]

            for status in reader:
                has_next_page = True

                while has_next_page:

                    node = "/{}/comments".format(status['status_id'])
                    after = '' if after is '' else "&after={}".format(after)
                    base_url = base + node + parameters + after

                    url = getFacebookCommentFeedUrl(base_url)
                    comments = json.loads(request_until_succeed(url))
                    reactions = getReactionsForComments(base_url)

                    for comment in comments['data']:
                        comment_data = processFacebookComment(
                            comment, status['status_id'])
                        reactions_data = reactions[comment_data[0]]

                        # calculate thankful/pride through algebra
                        num_special = comment_data[5] - sum(reactions_data)

                        # Enforce length criteria
                        comment_message = comment_data[-2]
                        if len(comment_message) >= 50 and len(comment_message) <= 280:
                            w.writerow(comment_data + reactions_data + (num_special, ))


                        # NOTE: if short on data, use sub-comments, otherwise ignore for now
                        # if 'comments' in comment:
                        #     has_next_subpage = True
                        #     sub_after = ''
                        #
                        #     while has_next_subpage:
                        #         sub_node = "/{}/comments".format(comment['id'])
                        #         sub_after = '' if sub_after is '' else "&after={}".format(
                        #             sub_after)
                        #         sub_base_url = base + sub_node + parameters + sub_after
                        #
                        #         sub_url = getFacebookCommentFeedUrl(
                        #             sub_base_url)
                        #         sub_comments = json.loads(
                        #             request_until_succeed(sub_url))
                        #         sub_reactions = getReactionsForComments(
                        #             sub_base_url)
                        #
                        #         for sub_comment in sub_comments['data']:
                        #             sub_comment_data = processFacebookComment(
                        #                 sub_comment, status['status_id'], comment['id'])
                        #             sub_reactions_data = sub_reactions[
                        #                 sub_comment_data[0]]
                        #
                        #             num_sub_special = sub_comment_data[
                        #                 6] - sum(sub_reactions_data)
                        #
                        #             w.writerow(sub_comment_data +
                        #                        sub_reactions_data + (num_sub_special,))
                        #
                        #             num_processed += 1
                        #             if num_processed % 100 == 0:
                        #                 print("{} Comments Processed: {}".format(
                        #                     num_processed,
                        #                     datetime.datetime.now()))
                        #
                        #         if 'paging' in sub_comments:
                        #             if 'next' in sub_comments['paging']:
                        #                 sub_after = sub_comments[
                        #                     'paging']['cursors']['after']
                        #             else:
                        #                 has_next_subpage = False
                        #         else:
                        #             has_next_subpage = False

                        # output progress occasionally to make sure code is not
                        # stalling
                        num_processed += 1
                        if num_processed % 500 == 0:
                            print("{} Comments Processed: {}".format(
                                num_processed, datetime.datetime.now()))

                    if 'paging' in comments:
                        if 'next' in comments['paging']:
                            after = comments['paging']['cursors']['after']
                        else:
                            has_next_page = False
                    else:
                        has_next_page = False

        print("\nDone!\n{} Comments Processed in {}".format(
            num_processed, datetime.datetime.now() - scrape_starttime))


if __name__ == '__main__':
    scrapeFacebookPageFeedComments(file_id, access_token)


# The CSV can be opened in all major statistical programs. Have fun! :)
