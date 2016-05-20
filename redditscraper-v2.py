import praw
import pprint
import datetime
import sys
import psycopg2

# Set default encoding to utf-8 so we dont get ascii encoding errors
reload(sys)
sys.setdefaultencoding('utf-8')

SEARCH_TERMS        = ('BoycottStarWarsVII',)
POSTGRES_USER       = 'ubuntu'
POSTGRES_DB         = 'redditdata001'


r = praw.Reddit(user_agent='linux:academic.research.comments.scraper:v0.2.3 (by /u/mergeandvary)')

# Define Vars
submission_db_dict = {}
comment_db_dict = {}
author_db_dict = {}
subreddit_db_dict = {}
author_collection = ()

def addCommentRegression(submission,comment,parentid):
    global submission_db_dict
    global comment_db_dict
    # Make sure it is a comment rather than morecomments object
    if isinstance(comment, praw.objects.Comment):
        try:
            comment_dict = {}
            comment_dict['SubmissionID']    = submission.id
            comment_dict['SubredditID']     = submission.subreddit_id
            # comment_dict['ParentID']        = comment.parent_id
            # Problem with mismatches, grab previous comment id instead but maybe this indicates deleted post?
            comment_dict['ParentID']        = parentid
            comment_dict['Author']          = comment.author
            comment_dict['Created']         = datetime.datetime.fromtimestamp(int(comment.created_utc)).strftime('%Y-%m-%d %H:%M:%S')
            comment_dict['Score']           = comment.score
            comment_dict['Removal_Reason']  = comment.removal_reason
            comment_dict['Report_Reasons']  = comment.report_reasons
            comment_dict['Edited']          = comment.edited
            comment_dict['Controversial']   = comment.controversiality
            comment_dict['Body']            = comment.body
            comment_db_dict[comment.id] = comment_dict
            print 'Added comment ID: ' + str(comment.id)
        except Exception as e: print(e)

        addAuthor(comment.author)

        # Regression for comments
        if comment.replies:
            for reply in comment.replies:
                addCommentRegression(submission,reply,comment.id)
    else:
        print 'More Comments OBJECT'

def addAuthor(author):
    global author_collection
    try:
        if author not in author_collection:
            author_collection = author_collection + (author,)
            print 'Added Author: ' + str(author)
    except Exception as e: 
        print str(e) + ' for author ' + str(author) + ' adding as string instead of object'
        try:
            if str(author) not in author_collection:
                author_collection = author_collection + (str(author),)
                print 'Added Shadow_Banned Author as String: ' + str(author)
        except Exception as e: print str(e) + ' Failed to add author as string instead of object'
    # NOTICE: It seems that some users who are shadow banned will drop a 404 HTTP error
    # Not really sure what to do with these authors !

def addSubreddit(subreddit_id,name):
    global subreddit_db_dict
    try:
        if subreddit_id not in subreddit_db_dict:
            subreddit_db_dict[subreddit_id] = {'Name': str(name)}
            print 'Added subreddit: ' + str(name)
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
                submission_db_dict[submission.id] = submission_dict
                print 'Added Submission ID: ' + str(submission.id)
            except Exception as e: print(e)

            try:
                addAuthor(submission.author)
                addSubreddit(submission.subreddit_id,submission.subreddit)
            except Exception as e: print(e)

            if submission.comments:
                for comment in submission.comments:
                    addCommentRegression(submission,comment,submission.id)

for author in author_collection:
    print 'Collecting Author' + str(author)
    try:
        if author.name:
            author_dict = {}
            author_dict['Created']           = datetime.datetime.fromtimestamp(int(author.created_utc)).strftime('%Y-%m-%d')
            author_dict['Comment_Karma']     = author.comment_karma
            author_dict['Link_Karma']        = author.link_karma
            author_dict['Is_Mod']            = author.is_mod
            author_dict['Is_404']   = 'False'
            author_db_dict[str(author.name)] = author_dict
    except Exception as e: 
        print(e)
        try:
            author_db_dict[str(author)] = {'Created': '', 'Comment_Karma': '0', 'Link_Karma': '0', 'Is_Mod': '', 'Is_404': 'True'}
        except Exception as e: print(e)
        # THIS IS 404 ERROR for AUTHOR to make sure we still have an entry. If fails again then None object
        # If get a 404 dump author as string instead as per added in collection function
        # 404 occurs for authors who are shadow banned so add this info
author_db_dict['None'] = {'Created': '', 'Comment_Karma': '0', 'Link_Karma': '0', 'Is_Mod': '', 'Is_404': 'False'}
        

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
    cur.execute("DROP TABLE IF EXISTS Subreddits CASCADE")
    cur.execute("DROP TABLE IF EXISTS Post CASCADE")
    cur.execute("DROP TABLE IF EXISTS Submission CASCADE")
    cur.execute("DROP TABLE IF EXISTS Comment CASCADE")
    cur.execute("DROP TABLE IF EXISTS Author CASCADE")
    cur.execute("DROP TABLE IF EXISTS Subreddit CASCADE")
    
    cur.execute("CREATE TABLE Subreddit(SubredditID TEXT PRIMARY KEY, Subreddit_Name TEXT)")
    cur.execute("CREATE TABLE Author(Author TEXT PRIMARY KEY, Comment_Karma INT, Created TEXT, Is_Mod TEXT, Link_Karma INT, Is_404 TEXT)")
    cur.execute("CREATE TABLE Post(PostID TEXT PRIMARY KEY, Author TEXT REFERENCES Author, Created TEXT, Bodytext TEXT, SubredditID TEXT REFERENCES Subreddit)")
    cur.execute("CREATE TABLE Submission(PostID TEXT PRIMARY KEY REFERENCES Post, Created TEXT, Score INT, Title TEXT)")
    cur.execute("CREATE TABLE Comment(PostID TEXT PRIMARY KEY REFERENCES Post, Controversial INT, Edited TEXT, ParentID TEXT REFERENCES Post, Removal_Reason TEXT, Report_Reasons TEXT, Score INT, SubmissionID TEXT REFERENCES Submission)")
    # PROBLEM how do we deal with parent comments that are the submission?
    # SOLUTION Use supertype

    # ADD SUBREDDITS
    table = ()
    for key, value in subreddit_db_dict.iteritems():
        entry = (str(key),
                 str(value['Name']),
                 )
        table = table + (entry,)
    query = "INSERT INTO Subreddit (SubredditID, Subreddit_Name) VALUES (%s, %s)"
    cur.executemany(query, table)

    # ADD AUTHORS
    table = ()
    for key, value in author_db_dict.iteritems():
        entry = (str(key),
                 int(value['Comment_Karma']),
                 str(value['Created']),
                 str(value['Is_Mod']),
                 int(value['Link_Karma']),
                 str(value['Is_404']),
                 )
        table = table + (entry,)
    query = "INSERT INTO Author (Author, Comment_Karma, Created, Is_Mod, Link_Karma, Is_404) VALUES (%s, %s, %s, %s, %s, %s)"
    cur.executemany(query, table)

    # ADD POSTS
    table = ()
    for key, value in submission_db_dict.iteritems():
        entry = (str(key), 
                 str(value['Author']), 
                 str(value['Selftext']), 
                 str(value['SubredditID'])
                 )
        table = table + (entry,)    
    for key, value in comment_db_dict.iteritems():
        entry = (str(key), 
                 str(value['Author']), 
                 str(value['Body']), 
                 str(value['SubredditID'])
                 )
        table = table + (entry,)    
    query = "INSERT INTO Post (PostID, Author, Bodytext, SubredditID) VALUES (%s, %s, %s, %s)"
    cur.executemany(query, table)

    # ADD SUBMISSIONS
    table = ()
    for key, value in submission_db_dict.iteritems():
        entry = (str(key), 
                 str(value['Created']), 
                 int(value['Score']), 
                 str(value['Title'])
                 )
        table = table + (entry,)    
    query = "INSERT INTO Submission (PostID, Created, Score, Title) VALUES (%s, %s, %s, %s)"
    cur.executemany(query, table)

    # ADD COMMENTS
    table = ()
    for key, value in comment_db_dict.iteritems():
        entry = (str(key), 
                 int(value['Controversial']), 
                 str(value['Edited']), 
                 str(value['ParentID']),
                 str(value['Removal_Reason']),
                 str(value['Report_Reasons']),
                 int(value['Score']),
                 str(value['SubmissionID']),
                 )
        table = table + (entry,)
    query = "INSERT INTO Comment (PostID, Controversial, Edited, ParentID, Removal_Reason, Report_Reasons, Score, SubmissionID) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
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