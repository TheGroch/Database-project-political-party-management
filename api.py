import json
import psycopg2
import sys
import argparse

db_open = False
cursor = None
connection = None

# Print output and commit queries
def print_error():
    connection.rollback()
    print("{\"status\": \"ERROR\"}")
    return

def print_ok():
    connection.commit()
    print("{\"status\": \"OK\"}")
    return

def print_data_ok(fetched):
    connection.commit()
    result = "{\"status\": \"OK\",\n \"data\": "
    j = json.dumps(fetched)
    print(result + j + " }")
    return

# Read and redirect to associated function
def read(args):
    global db_open
    for line in sys.stdin:
        try:
            jason = json.loads(line.rstrip('\n'))
        except:
            print_error()
            continue
        if 'open' in jason:
            connect(jason['open'], args)
            continue
        elif not db_open:
            print_error()
            continue
        
        if 'leader' in jason:
            add_leader(jason['leader'], args)
            continue
        elif args.init:
            print_error()
            continue
        if 'protest' in jason or 'support' in jason:
            protest_support(jason)
        elif 'upvote' in jason or 'downvote' in jason:
            upvote_downvote(jason)
        elif 'actions' in jason:
            actions(jason['actions'])
        elif 'projects' in jason:
            projects(jason['projects'])
        elif 'votes' in jason:
            votes(jason['votes'])
        elif 'trolls' in jason:
            trolls(jason['trolls'])
        else:
            print_error()
    return

# Connect to database
def connect(jason, args):
    global db_open, cursor, connection
    db = ""
    login = ""
    password = ""
    if all (k in jason for k in ('database', 'login', 'password')):
        db = jason['database']
        login = jason['login']
        password = jason['password']
    else:            
        print_error()
        db_open = False
        return

    try:
        connection = psycopg2.connect(user=login, password=password, database=db)
        cursor = connection.cursor()
    except (Exception, psycopg2.Error):
        print_error()
        db_open = False
        return

    db_open = True
    if args.init:
        initialize()
    print_ok()
    return

#Initialize database
def initialize():
    global cursor, connection
    cursor.execute(open('model.sql').read())
    return

# Add leader (function: leader)
def add_leader(jason, args):
    global db_open, cursor, connection
    if not args.init:
        print_error()
        return
    
    if all (k in jason for k in ('timestamp', 'password', 'member')):
        timestamp = jason['timestamp']
        password = jason['password']
        uid = jason['member']
    else:
        print_error()
        return
    if is_unique_id(uid):
       cursor.execute('''INSERT INTO member (id, passwd, last_timestamp, is_leader) 
                         VALUES (%s, crypt(%s, gen_salt('bf')), to_timestamp(%s), %s);''', (uid, password, timestamp, True)) 
       add_id(uid)
       print_ok()
    else:
        print_error()
        return

# Add action (function: protest/support)
def protest_support(jason):
    key = list(jason.keys())[0]
    if all (k in jason[key] for k in ('timestamp', 'password', 'member', 'action', 'project')):
        timestamp = jason[key]['timestamp']
        password = jason[key]['password']
        uid = jason[key]['member']
        action = jason[key]['action']
        project = jason[key]['project']
    else:
        print_error()
        return
    # Check if given ids are unique
    ids = []
    ids.append(action)
    ids.append(project)
    ids.append(uid)   
    if 'authority' in jason[key]:
        ids.append(jason[key]['authority'])
    if len(ids) > len(set(ids)):
        print_error()
        return

    if check_member(uid, password, timestamp) and is_unique_id(action):
        if is_unique_id(project):
            if 'authority' in jason[key]: 
                add_project(project, jason[key]['authority'])
            else:
                print_error()
                return
        add_action(action, key, project, uid)
        print_ok()
        return
    else:
        print_error()
        return

# Add vote (function: upvote/downvote)
def upvote_downvote(jason):
    key = list(jason.keys())[0]
    if all (k in jason[key] for k in ('timestamp', 'password', 'member', 'action')):
        timestamp = jason[key]['timestamp']
        password = jason[key]['password']
        uid = jason[key]['member']
        action = jason[key]['action']
    else:
        print_error()
        return
    if check_member(uid, password, timestamp) and check_action(action) and check_vote(uid, action):
        add_vote(key, uid, action)
        print_ok()
        return
    else:
        print_error()
        return
    return

# Print actions (function: actions)
def actions(jason):
    if all (k in jason for k in ('timestamp', 'password', 'member')):
        timestamp = jason['timestamp']
        password = jason['password']
        uid = jason['member']
    else:
        print_error()
        return
    if check_member(uid, password, timestamp, True):
        action_type = None
        authority = None
        project = None
        if 'type' in jason:
            action_type = jason['type']
        if 'project' in jason:
            project = jason['project']
        if 'authority' in jason:
            authority = jason['authority']
        if project is not None and authority is not None:
            print_error()
            return
        query = open('actions.sql').read() +'\n'
        if action_type is not None:
            query = query + '''WHERE action_type = %s\n'''
        if project is not None:
            query = query + '''WHERE projects.id = %s\n'''
        if authority is not None:
            query = query + '''WHERE authority_ID = %s\n'''
        query = query + '''ORDER BY actions.id ASC;'''
        
        if action_type is None and authority is None and project is None:
            cursor.execute(query)
        elif action_type is not None and project is None and authority is None:
            cursor.execute(query)
        elif action_type is not None and project is not None:
            cursor.execute(query)
        elif action_type is not None and authority is not None:
            cursor.execute(query, (action_type, authority))
        elif project is not None:
            cursor.execute(query, [project])
        elif authority is not None:
            cursor.execute(query, [authority])
        else:
            print_error()
            return
        print_data_ok(cursor.fetchall())
        return
    print_error()
    return

# Prints projects (function: projects)
def projects(jason):
    if all (k in jason for k in ('timestamp', 'password', 'member')):
        timestamp = jason['timestamp']
        password = jason['password']
        uid = jason['member']
    else:
        print_error()
        return
    if check_member(uid, password, timestamp, True):
        authority = None
        if 'authority' in jason:
            authority = jason['authority']
        if authority is None:
            cursor.execute('''SELECT * FROM projects ORDER BY projects.id ASC;''')
        else:
            cursor.execute('''SELECT * FROM projects WHERE authority_ID = %s ORDER BY projects.id ASC;''', [authority])
        print_data_ok(cursor.fetchall())
        return
    else:
        print_error()
        return

# Prints votes (function: votes)
def votes(jason):
    if all (k in jason for k in ('timestamp', 'password', 'member')):
        timestamp = jason['timestamp']
        password = jason['password']
        uid = jason['member']
    else:
        print_error()
        return
    if check_member(uid, password, timestamp, True):
        action = None
        project = None
        if 'action' in jason:
            action = jason['action']
        if 'project' in jason:
            project = jason['project']
        if action is not None and project is not None:
            print_error()
            return
        if action is None and project is None:
            cursor.execute(open('votes.sql').read())
        elif action is not None and project is None:
            cursor.execute(open('votes_action.sql').read(), (action, action))
        elif action is None and project is not None:
            cursor.execute(open('votes_project.sql').read(), (project, project))
        print_data_ok(cursor.fetchall())
    else:
        print_error()
        return
    return

# Prints trolls (function: trolls)
def trolls(jason):
    if 'timestamp' in jason:
        timestamp = jason['timestamp']
    else:
        print_error()
        return
    cursor.execute('''SELECT id, upvotes, downvotes, last_timestamp FROM member
                    WHERE downvotes > upvotes
                    ORDER BY downvotes - upvotes DESC, id ASC;''')
    data = []
    for i in cursor.fetchall():
        if is_frozen(timestamp, i[3]):
            data.append([i[0], i[1], i[2], "false"])
        else:
            data.append([i[0], i[1], i[2], "true"])
    result = "{ \"status\": \"OK\",\n \"data\": " + json.dumps(data) + " }"
    print(result)
    return


# All the helper functions:
def is_unique_id(uid):
    cursor.execute('''SELECT id FROM identifier WHERE id = %s''', [uid])
    if cursor.fetchone() is None:
        return True
    return False

def add_id(uid):
    global cursor, connection
    cursor.execute('''INSERT INTO identifier (id) VALUES (%s)''', [uid])
    return

def check_member(uid, password, timestamp, leader = False):
    global cursor
    cursor.execute('''SELECT id, passwd, last_timestamp, is_leader FROM member
                    WHERE id = %s AND passwd = crypt(%s, passwd);''', (uid, password))
    record = cursor.fetchone()
    if record is None:
        if is_unique_id(uid):
            add_member(uid, password, timestamp)
            return True
        else:
            return False
    if is_frozen(timestamp, str(record[2])):
        return False
    update_timestamp(uid, timestamp)
    if leader:
        return record[3]
    return True

def is_frozen(timestamp_new, timestamp_last):
    global cursor
    cursor.execute('''SELECT EXTRACT (YEAR FROM AGE((SELECT TO_TIMESTAMP(%s)), %s));''', (timestamp_new, timestamp_last))
    if cursor.fetchone()[0] >= 1:
        return True
    return False

def add_member(uid, password, timestamp):
    cursor.execute('''INSERT INTO member (id, passwd, last_timestamp, is_leader) 
                    VALUES (%s, crypt(%s, gen_salt('bf')), to_timestamp(%s), %s);''', (uid, password, timestamp, False)) 
    add_id(uid)
    return

def update_timestamp(uid, timestamp):
    global cursor, connection
    cursor.execute('''UPDATE member set last_timestamp = to_timestamp(%s) WHERE id = %s;''', (uid, timestamp))
    return

def add_project(project, authority):
    global cursor
    cursor.execute('''INSERT INTO projects (id, authority_ID) VALUES (%s, %s);''', (project, authority))
    add_id(project)
    if is_unique_id(authority):
        add_id(authority)            
    return

def add_action(action, action_type, uid, project):
    global cursor
    cursor.execute('''INSERT INTO actions (id, action_type, project_id, member_id) 
                    VALUES (%s, %s, %s, %s);''', (action, action_type, uid, project))
    add_id(action)
    return

def check_action(action):
    cursor.execute('''SELECT id FROM actions WHERE id = %s;''', [action])
    if cursor.fetchone() is None:
        return False
    return True

def check_vote(uid, action):
    cursor.execute('''SELECT member_ID, action_ID FROM votes WHERE member_ID = %s AND action_ID = %s;''', (uid, action))
    if cursor.fetchone() is None:
        return True
    return False

def add_vote(vote_type, uid, action):
    cursor.execute('''INSERT INTO votes (vote_type, member_ID, action_ID) VALUES (%s, %s, %s)''', (vote_type, uid, action))
    if vote_type == 'downvote':
        cursor.execute('''WITH member_to_update AS
                        (
                            SELECT member_ID FROM actions
                            WHERE id = %s
                        )
                        UPDATE member SET downvotes = downvotes + 1 
                        FROM member_to_update WHERE member.id = member_to_update.member_ID;''', [action])
    else:
        cursor.execute('''WITH member_to_update AS
                        (
                            SELECT member_ID FROM actions
                            WHERE id = %s
                        )
                        UPDATE member SET upvotes = upvotes + 1 
                        FROM member_to_update WHERE member.id = member_to_update.member_ID;''', [action])
    return

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--init", help="initialize databese", action="store_true")
    args = parser.parse_args()
    read(args)

if __name__ == "__main__":
    main()