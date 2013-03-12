import base64
import functools
import json
import os

from flask import Flask
from flask import g
from flask import render_template
from flask import request
from flask import Response

import db

app = Flask(__name__)


@app.before_request
def before_request():
    conn = db.get_conn()
    g.db = conn.cursor()

    g.db.execute("""SELECT * FROM netconfigs""")
    res = g.db.fetchall()
    g.nc_map = {}
    for ncid, ncname in res:
        g.nc_map[ncname] = ncid
    g.ncid_map = dict((v, k) for k, v in g.nc_map.items())

    g.db.execute("""SELECT * FROM operating_systems""")
    res = g.db.fetchall()
    g.os_map = {}
    for osid, osname in res:
        g.os_map[osname] = osid
    g.osid_map = dict((v, k) for k, v in g.os_map.items())


@app.teardown_request
def teardown_request(exception):
    conn = g.db.connection
    conn.commit()
    conn.close()


def auth_ok():
    if not request.authorization:
        return False

    authstr = os.getenv('SRPUSH_AUTH', '')
    if not authstr:
        return False

    try:
        authjson = base64.b64decode(authstr)
    except:
        return False

    try:
        auth = json.loads(authjson)
    except:
        return False

    if request.authorization.username not in auth:
        return False

    pw = auth[request.authorization.username]
    if request.authorization.password != pw:
        return False

    return True


def authenticated(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not auth_ok():
            return Response('Authentication required', 401,
                            {'WWW-Authenticate':
                             'Basic realm="Stone Ridge Pushers"'})
        return f(*args, **kwargs)
    return decorated


@app.route('/mark_handled', methods=['POST'])
@authenticated
def mark_handled():
    handled_ids = request.form.getlist('id')

    for pushid in handled_ids:
        g.db.execute("""UPDATE pushes SET handled = 't' WHERE id = %s""",
                     (pushid,))

    return 'ok'


@app.route('/list_unhandled', methods=['GET'])
@authenticated
def list_unhandled():
    g.db.execute("""SELECT * FROM pushes WHERE handled = 'f'""")
    res = g.db.fetchall()

    rval = []

    for r in res:
        pushid, srid, ldap, sha, _ = r

        netconfigs = []
        g.db.execute("""SELECT ncid FROM push_netconfigs WHERE pushid = %s""",
                     (pushid,))
        nres = g.db.fetchall()
        for nr in nres:
            netconfigs.append(g.ncid_map[nr[0]])

        operating_systems = []
        g.db.execute("""SELECT osid FROM push_operating_systems
                        WHERE pushid = %s""",
                     (pushid,))
        ores = g.db.fetchall()
        for or_ in ores:
            operating_systems.append(g.osid_map[or_[0]])

        rval.append({'pushid': pushid,
                     'srid': srid,
                     'ldap': ldap,
                     'sha': sha,
                     'netconfigs': netconfigs,
                     'operating_systems': operating_systems})

    return json.dumps(rval)


@app.route('/srpush', methods=['POST'])
@authenticated
def srpush():
    srid = request.form.get('srid', None)
    ldap = request.form.get('ldap', None)
    sha = request.form.get('sha', None)
    netconfigs = request.form.getlist('netconfig')
    operating_systems = request.form.getlist('operating_system')

    if not srid or not ldap or not sha or not netconfigs or \
            not operating_systems:
        raise Exception('Missing info from request!')

    g.db.execute("""INSERT INTO pushes (srid, ldap, sha)
                    VALUES (%s, %s, %s)""",
                 (srid, ldap, sha))
    g.db.execute("""SELECT LASTVAL()""")
    res = g.db.fetchall()
    pushid = res[0][0]

    ncids = set()
    osids = set()

    for nc in netconfigs:
        ncid = g.nc_map[nc]
        ncids.add(ncid)
        g.db.execute("""INSERT INTO push_netconfigs (pushid, ncid)
                        VALUES (%s, %s)""", (pushid, ncid))

    for ops in operating_systems:
        osid = g.os_map[ops]
        osids.add(osid)
        g.db.execute("""INSERT INTO push_operating_systems (pushid, osid)
                        VALUES (%s, %s)""", (pushid, osid))

    for ncid in ncids:
        for osid in osids:
            g.db.execute("""INSERT INTO push_status (pushid, osid, ncid,
                            status) VALUES (%s, %s, %s, 'waiting')""",
                         (pushid, osid, ncid))

    return 'ok'


@app.route('/status/update', methods=['POST'])
@authenticated
def status_update():
    srid = request.form.get('srid', None)
    ncid = request.form.get('nc', None)
    osid = request.form.get('os', None)
    status = request.form.get('status', None)

    if not srid or not status:
        raise Exception('Missing info from request!')

    g.db.execute("""SELECT id FROM pushes WHERE srid = %s""",
                 (srid,))
    pushid = g.db.fetchall()[0][0]

    query = """UPDATE push_status SET status = %s WHERE pushid = %s"""
    args = [status, pushid]
    if ncid:
        query += """ AND ncid = %s"""
        args.append(ncid)
    if osid:
        query += """ AND osid = %s"""
        args.append(osid)

    g.db.execute(query, args)

    return 'ok'


@app.route('/')
def index():
    g.db.execute("""SELECT id, ldap, sha FROM pushes
                    ORDER BY id DESC
                    LIMIT 10""")
    res = g.db.fetchall()
    pushes = []
    for r in res:
        pushid, ldap, sha = r
        g.db.execute("""SELECT ncid, osid, status FROM push_status
                        WHERE id = %s""", (pushid,))
        stats = g.db.fetchall()
        push = {}
        for s in stats:
            ncid, osid, status = s
            push[(ncid, osid)] = status
        rendered_push = render_template('push_status.html', push=push,
                                        ldap=ldap, sha=sha)
        pushes.append(rendered_push)
    return render_template('status.html', pushes=pushes)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
