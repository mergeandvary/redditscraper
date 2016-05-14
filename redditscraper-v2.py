import praw
import pprint
import datetime
import sys
import psycopg2

# Set default encoding to utf-8 so we dont get ascii encoding errors
reload(sys)
sys.setdefaultencoding('utf-8')

SEARCH_TERMS        = ('INeedMasculism', 'INeedMasculinism', 'INeedMasculismBecause', 'INeedMasculinismBecause')
POSTGRES_USER       = 'ubuntu'
POSTGRES_DB         = 'redditdata001'


r = praw.Reddit(user_agent='linux:academic.research.comments.scraper:v0.2.3 (by /u/mergeandvary)')

# Define Vars
submission_db_dict = {}
comment_db_dict = {}
author_db_dict = {}
author_collection = ()

def addCommentRegression(submission,comment):
    global submission_db_dict
    global comment_db_dict
    # Make sure it is a comment rather than morecomments object
    if isinstance(comment, praw.objects.Comment):
        try:
            comment_dict = {}
            comment_dict['SubmissionID']    = submission.id
            comment_dict['ParentID']        = comment.parent_id
            comment_dict['Author']          = comment.author
            comment_dict['Created']         = datetime.datetime.fromtimestamp(int(comment.created_utc)).strftime('%Y-%m-%d %H:%M:%S')
            comment_dict['Score']           = comment.score
            comment_dict['Removal_Reason']  = comment.removal_reason
            comment_dict['Report_Reasons']  = comment.report_reasons
            comment_dict['Edited']          = comment.edited
            comment_dict['Controversial']   = comment.controversiality
            comment_dict['Body']            = comment.body

            addAuthor(comment.author)
            comment_db_dict[comment.id] = comment_dict
            print 'Added comment ID: ' + str(comment.id)
        except Exception as e: print(e)

        # Regression for comments
        if comment.replies:
            for reply in comment.replies:
                addCommentRegression(submission,reply)
    else:
        print 'More Comments OBJECT'

def addAuthor(author):
    global author_collection
    try:
        if author not in author_collection:
            author_collection = author_collection + (author,)
            print 'Added Author: ' + str(author)
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
            # Get The Submissions
            try:
                submission_dict = {}
                submission_dict['Title']        = submission.title
                submission_dict['Author']       = submission.author
                submission_dict['Created']      = datetime.datetime.fromtimestamp(int(submission.created_utc)).strftime('%Y-%m-%d %H:%M:%S')
                submission_dict['SubredditID']  = submission.subreddit_id
                submission_dict['Score']        = submission.score
                submission_dict['Selftext']     = submission.selftext

                addAuthor(submission.author)
                submission_db_dict[submission.id] = submission_dict
                print 'Added Submission ID: ' + str(submission.id)
            except Exception as e: print(e)

            if submission.comments:
                for comment in submission.comments:
                    addCommentRegression(submission,comment)

for author in author_collection:
    try:
        author_dict = {}
        author_dict['Created']          = datetime.datetime.fromtimestamp(int(author.created_utc)).strftime('%Y-%m-%d')
        author_dict['Comment_Karma']    = author.comment_karma
        author_dict['Link_Karma']       = author.link_karma
        author_dict['Is_Mod']           = author.is_mod
        author_db_dict[author.name]     = author_dict
    except Exception as e: print(e)

# LOGFILES
print 'WRITING in LOG Textfiles'
submission_db_logfile = open('submission_db_logfile.txt', 'w')
pprint.pprint(submission_db_dict, submission_db_logfile)
comment_db_logfile = open('comment_db_logfile.txt', 'w')
pprint.pprint(comment_db_dict, comment_db_logfile)
author_db_logfile = open('author_db_logfile.txt', 'w')
pprint.pprint(author_db_dict, author_db_logfile)

print 'ADDING in POSTGRESQL Database'
# POSTGRESQL
con = None

try:     
    con = psycopg2.connect(database=POSTGRES_DB, user=POSTGRES_USER) 
    cur = con.cursor()

    # CLEAN UP TABLES
    cur.execute("DROP TABLE IF EXISTS Submissions CASCADE")
    cur.execute("DROP TABLE IF EXISTS Comments CASCADE")
    cur.execute("DROP TABLE IF EXISTS Authors CASCADE")
    
    cur.execute("CREATE TABLE Submissions(SubmissionID TEXT PRIMARY KEY, Author TEXT, Created TEXT, Score INT, Selftext TEXT, SubredditID TEXT, Title TEXT)")
    cur.execute("CREATE TABLE Comments(CommentID TEXT PRIMARY KEY, Author TEXT, Body TEXT, Controversial INT, Created TEXT, Edited TEXT, ParentID TEXT, Removal_Reason TEXT, Report_Reasons TEXT, Score INT, SubmissionID TEXT REFERENCES Submissions)")
    cur.execute("CREATE TABLE Authors(Author TEXT PRIMARY KEY, Comment_Karma INT, Created TEXT, Is_Mod TEXT, Link_Karma INT)")

    # ADD SUBMISSIONS   
    table = ()
    for key, value in submission_db_dict.iteritems():
        entry = (str(key), 
                 str(value['Author']), 
                 str(value['Created']), 
                 int(value['Score']), 
                 str(value['Selftext']), 
                 str(value['SubredditID']), 
                 str(value['Title'])
                 )
        table = table + (entry,)    
    query = "INSERT INTO Submissions (SubmissionID, Author, Created, Score, Selftext, SubredditID, Title) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    cur.executemany(query, table)

    # ADD COMMENTS
    table = ()
    for key, value in comment_db_dict.iteritems():
        entry = (str(key), 
                 str(value['Author']), 
                 str(value['Body']), 
                 int(value['Controversial']), 
                 str(value['Created']), 
                 str(value['Edited']), 
                 str(value['ParentID']),
                 str(value['Removal_Reason']),
                 str(value['Report_Reasons']),
                 int(value['Score']),
                 str(value['SubmissionID']),
                 )
        table = table + (entry,)
    query = "INSERT INTO Comments (CommentID, Author, Body, Controversial, Created, Edited, ParentID, Removal_Reason, Report_Reasons, Score, SubmissionID) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cur.executemany(query, table)

    # ADD AUTHORS
    table = ()
    for key, value in author_db_dict.iteritems():
        entry = (str(key),
                 int(value['Comment_Karma']),
                 str(value['Created']),
                 str(value['Is_Mod']),
                 int(value['Link_Karma']),
                 )
        table = table + (entry,)
    query = "INSERT INTO Authors (Author, Comment_Karma, Created, Is_Mod, Link_Karma) VALUES (%s, %s, %s, %s, %s)"
    cur.executemany(query, table)

    # COMMIT CHANGES
    con.commit()

except psycopg2.DatabaseError, e:
    if con:
        con.rollback()
    print 'Error %s' % e    
    sys.exit(1)
    
finally:
    if con:
        con.close()