import praw
import pprint
import datetime
import xlwt
import sys

# TODO Downvotes no longer supplied, ups is same as score, need to get ratio
# ratio = r.get_submission(submission.permalink).upvote_ratio
# Also, see here: https://www.reddit.com/r/announcements/comments/28hjga/reddit_changes_individual_updown_vote_counts_no/
# Might just need to remove down/ups altogether

# Set default encoding to utf-8 so we dont get ascii encoding errors
reload(sys)
sys.setdefaultencoding('utf-8')

# COMMAND LINE ARGUMENTS
# comments 
# submissions
# combined
# authors

SUBMISSIONS_DB_CAT      = ('ID', 'TITLE', 'AUTHOR', 'URL', 'TIMESTAMP', 'SUBREDDIT', 'SUBREDDIT ID', 'SCORE', 'COMMENT')
COMMENTS_DB_CAT         = ('ID', 'AUTHOR', 'SCORE', 'TIMESTAMP', 'SUBREDDIT', 'COMMENT')
AUTHORS_DB_CAT          = ('AUTHOR', 'CREATED', 'COMMENT KARMA', 'LINK KARMA')
SEARCH_TERMS            = ('INeedMasculinism', 'INeedMasculism')

r = praw.Reddit(user_agent='linux:academic.research.comments.scraper:v0.0.9 (by /u/mergeandvary)')

submissions_db = (SUBMISSIONS_DB_CAT,)
comments_db = (COMMENTS_DB_CAT,)
authors_db = (AUTHORS_DB_CAT,)

individual_submissions_db = ()
authors = ()


wb = xlwt.Workbook(encoding='utf-8')

def addCommentCommentsdb(submission,comment):
    comment_db = ()
    global authors
    global comments_db
    global submission_comments_db
    if isinstance(comment, praw.objects.Comment):
        try:
            # Collect authors info for authors_db
            if comment.author not in authors:
                authors = authors + (comment.author,)

            comment_db = comment_db + (str(comment.id),)
            comment_db = comment_db + (str(comment.author),)
            comment_db = comment_db + (str(comment.score),)
            comment_db = comment_db + (str(datetime.datetime.fromtimestamp(int(comment.created_utc)).strftime('%Y-%m-%d %H:%M:%S')),)
            comment_db = comment_db + (str(submission.subreddit),)
            comment_db = comment_db + (str(comment.body),)
            comments_db = comments_db + (comment_db,)
            submission_comments_db = submission_comments_db + (comment_db,)
            print 'Added Comment ID: ' + str(comment.id) 
            if comment.replies:
                for reply in comment.replies:
                    addCommentCommentsdb(submission,reply)
        except Exception as e: print(e)

for searchterm in SEARCH_TERMS:
    print 'Searching for term: ' + str(searchterm)
    submissions = r.search(searchterm, subreddit=None, sort=None, syntax=None, period=None)
    for submission in submissions:
        # Make sure the submission isn't a bot copying another submission
        title = str(submission.title)
        if title.startswith('[COPY]'):
            print 'Submission COPY - Skipping...'
        else:
            submission_db = ()

            # Make a separate db page for submission details only
            try:
                submission_db = submission_db + (str(submission.id),)
                submission_db = submission_db + (str(submission.title),)
                submission_db = submission_db + (str(submission.author),)
                submission_db = submission_db + (str(submission.url),)
                submission_db = submission_db + (str(datetime.datetime.fromtimestamp(int(submission.created_utc)).strftime('%Y-%m-%d %H:%M:%S')),)
                submission_db = submission_db + (str(submission.subreddit),)
                submission_db = submission_db + (str(submission.subreddit_id),)
                submission_db = submission_db + (str(submission.score),)
                submission_db = submission_db + (str(submission.selftext),)
                submissions_db = submissions_db + (submission_db,)
                print 'Added Submission ID: ' + str(submission.id)
            except Exception as e: print(e)

            # Add the info about the submission that is same as comment because it is first comment
            comment_db = ()
            try:
                # Collect authors info for authors_db
                if submission.author not in authors:
                    authors = authors + (submission.author,)

                # Submission Comments
                comment_db = comment_db + (str(submission.id),)
                comment_db = comment_db + (str(submission.author),)
                comment_db = comment_db + (str(submission.score),)
                comment_db = comment_db + (str(datetime.datetime.fromtimestamp(int(submission.created_utc)).strftime('%Y-%m-%d %H:%M:%S')),)
                comment_db = comment_db + (str(submission.subreddit),)
                comment_db = comment_db + (str(submission.title) + str(submission.selftext),) #Add title to selftext because this can be considered whole comment. Some reddit comments don't have body text or use videos or images.
                comments_db = comments_db + (comment_db,)
            except Exception as e: print(e)

            # Reset comment_db tuple so we can also create individual comment thread submission pages
            # First lets add the submission to the top
            # And make sure to add category column info
            submission_comments_db = ()
            submission_comments_db = submission_comments_db + (COMMENTS_DB_CAT,)
            comment_db = ()
            try:
                comment_db = comment_db + (str(submission.id),)
                comment_db = comment_db + (str(submission.author),)
                comment_db = comment_db + (str(submission.score),)
                comment_db = comment_db + (str(datetime.datetime.fromtimestamp(int(submission.created_utc)).strftime('%Y-%m-%d %H:%M:%S')),)
                comment_db = comment_db + (str(submission.subreddit),)
                comment_db = comment_db + (str(submission.title) + str(submission.selftext),)
                submission_comments_db = submission_comments_db + (comment_db,)
            except Exception as e: print(e)

            # ADD the comments both to individual submission pages and the all comments pages
            # Need to move this out to a function for recursive checking of comment replies
            for comment in submission.comments:
                addCommentCommentsdb(submission,comment)

            individual_submissions_db = individual_submissions_db + (submission_comments_db,)

# Write out a submissions page with all submissions data
if 'submissions' in str(sys.argv) or len(sys.argv) == 1:
    print 'WRITING SUBMISSIONS'
    submissions_ws = wb.add_sheet("Submissions")
    for i, row in enumerate(submissions_db):
        for j, col in enumerate(row):
            submissions_ws.write(i, j, col)

# Write out a comments page with all comments data
if 'combined' in str(sys.argv) or len(sys.argv) == 1:
    print 'WRITING COMMENTS DB'
    comments_ws = wb.add_sheet("Comments")
    for i, row in enumerate(comments_db):
        for j, col in enumerate(row):
            comments_ws.write(i, j, col)

# Write out a comments page for each individual submission id
if 'comments' in str(sys.argv) or len(sys.argv) == 1:
    for individual_submission in individual_submissions_db:
        indsubid = individual_submission[1][0]
        print 'WRITING COMMENTS ID' + str(indsubid)
        submission_comments_ws = wb.add_sheet(str(indsubid))
        for i, row in enumerate(individual_submission):
            for j, col in enumerate(row):
                submission_comments_ws.write(i, j, col)

# Write the authors page
for author in authors:
    author_db = ()
    try:
        author_db = author_db + (str(author.name),)
        author_db = author_db + (str(datetime.datetime.fromtimestamp(int(author.created_utc)).strftime('%Y-%m-%d')),)
        author_db = author_db + (str(author.comment_karma),)
        author_db = author_db + (str(author.link_karma),)
        authors_db = authors_db + (author_db,)
    except Exception as e: print(e)

if 'authors' in str(sys.argv) or len(sys.argv) == 1:
    print 'WRITING AUTHORS DB'
    authors_ws = wb.add_sheet("Authors")
    for i, row in enumerate(authors_db):
        for j, col in enumerate(row):
            authors_ws.write(i, j, col)

wb.save("myworkbook.xls")