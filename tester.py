import praw
import pprint
import sys

# Set default encoding to utf-8 so we dont get ascii encoding errors
reload(sys)
sys.setdefaultencoding('utf-8')

r = praw.Reddit(user_agent='linux:academic.research.comments.scraper.tester:v0.2.3 (by /u/mergeandvary)')

SEARCH_TERMS        = ('INeedMasculism', )

for searchterm in SEARCH_TERMS:
    print 'Searching for term: ' + str(searchterm)
    submissions = r.search(searchterm, subreddit=None, sort=None, syntax=None, period=None)
    for submission in submissions:
        vars_logfile = open('vars_logfile.txt', 'w')
        pprint.pprint(vars(submission), vars_logfile)
        for comment in submission.comments:
            comment_vars_logfile = open('comment_vars_logfile.txt', 'w')
            pprint.pprint(vars(comment), comment_vars_logfile)