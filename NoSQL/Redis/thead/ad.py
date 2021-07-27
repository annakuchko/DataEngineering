import redis
from flask import Flask, session, request
from redis.sentinel import Sentinel
from time import strftime

app = Flask(__name__)
app.secret_key = '123'
r = redis.Redis(host='localhost', port=6379, db=0)

sentinel = Sentinel([('localhost', 16379)], socket_timeout=0.1)
master = sentinel.master_for('THEAD_CLUSTER', socket_timeout=0.1)

@app.route('/') 
def theanswer():
    day = strftime("%Y-%m-%d")
    user_visits = str(get_user_visits())
    try:
        cookies = str(list(request.cookies.to_dict(flat=False).values())[0])
    except:
        cookies = 'nan'
    master.set(f'page:index:counter:day:{day}:user:{cookies}', user_visits)
    return f"42 - {str(master.get(f'page:index:counter:day:{day}:user:{cookies}'))}"

def get_user_visits():
    if 'visits' in session:
        session['visits'] = session.get('visits') + 1  
    else:
        session['visits'] = 1
    return session.get('visits')
    
    