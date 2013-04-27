'''
Module for providing client and server management capabilities for Chef

Author: Gary Hetzel <ghetzel@outbrain.com>
Date:   2013-04-26

'''

import os
import salt
import salt.version
import salt.loader

try:
  import chef
  HAS_CHEF = True
except ImportError:
  pass


# format outputs
__outputter__ = {
  'run':          'txt',
  'list_nodes':   'txt',
  'list_clients': 'txt',
  'list_roles':   'txt'
}


def __virtual__():
  return 'chef'


def _get_chef_client():
  cmd = __salt__['cmd.which']('chef-client')
  if cmd is None:
    for bin in ['/opt/chef/bin/chef-client', '/opt/opscode/bin/chef-client']:
      if os.path.isfile(bin):
       cmd = bin
       break

  return cmd

def _get_api():
  if HAS_CHEF:
    api_host = __salt__['config.option']('chef.api.host', 'localhost')
    api_port = __salt__['config.option']('chef.api.port', '4000')
    api_key = __salt__['config.option']('chef.api.key')
    api_user = __salt__['config.option']('chef.api.user')

    return chef.ChefAPI('http://%s:%s' % (api_host, api_port), api_key, api_user)


def _call_api(method, endpoint):
  api = _get_api()

  if api is None:
    return False

  return api.api_request(method, endpoint)


# server commands (via API)
def list_nodes():
  '''
  Return a list of nodes registered on the Chef server
  '''
  return _call_api('GET', '/nodes').keys()


def list_roles():
  '''
  Return a list of roles registered on the Chef server
  '''
  return _call_api('GET', '/roles').keys()


def list_clients():
  '''
  Return a list of clients registered on the Chef server
  '''
  return _call_api('GET', '/clients').keys()


def list_data():
  '''
  Return a list of data bags on the Chef server
  '''
  return _call_api('GET', '/data').keys()


def search(index, query, start=0, limit=None, sort=None):
  '''
  Performs a search on the Chef server and returns the results

  CLI Example::
    salt '*' chef.search test 'key:search_pattern'


  index
    The Chef search index to query

  query
    The query you wish to perform

  start
    The result number from which to start

  limit
    The number of rows to be returned

  sort
    A sort string, such as "name DESC"

  '''

  endpoint = '/search/%s?q=%s&start=%d' % (index, query, start)

  if not limit is None:
    endpoint += '&rows=%d' % limit

  if not sort is None:
    endpoint += '&sort=%s' % sort

  return _call_api('GET', endpoint)


def node(name):
  '''
  Returns a single node
  '''
  return _call_api('GET', '/nodes/%s' % name)


def role(name):
  '''
  Returns a single role
  '''
  return _call_api('GET', '/roles/%s' % name)


def client(name):
  '''
  Returns a single client
  '''
  return _call_api('GET', '/clients/%s' % name)


def data(name, item=None):
  '''
  Returns a named databag and (optionally) a specific item within
  '''
  if item is None:
    return _call_api('GET', '/data/%s' % name)
  return _call_api('GET', '/data/%s/%s' % (name, item))


def delete_node(name):
  '''
  Delete a node
  '''
  return _call_api('DELETE', '/nodes/%s' % name)


def delete_role(name):
  '''
  Delete a role
  '''
  return _call_api('DELETE', '/roles/%s' % name)


def delete_client(name):
  '''
  Delete a client
  '''
  return _call_api('DELETE', '/clients/%s' % name)



# client commands

def version():
  '''
  Return the currently installed version of chef-client
  '''

  cmd = _get_chef_client()
  if cmd is None:
    return "Unable to locate chef-client executable."

  try:
    return __salt__['cmd.run'](cmd + ' -v').split(' ')[-1]
  except Exception:
    return False


def run(force=False,
        run_list=None,
        key_file=None,
        validation_key=None,
        server=None,
        environment=None,
        log_level=None,
        node_name=None,
        dry_run=None):
  '''
  Run chef-client on this minion

  force
    Don't check for the presence of a skipfile. Unconditionally runs chef-client.

    CLI Example::
      salt '*' chef.run force=True


  run_list
    Specify a run_list that overrides the existing one.

    CLI Example::
      salt '*' chef.run run_list='["role[global]", "recipe[awesome::server]"]'


  key_file
    Provide an alternative client key path.

    CLI Example::
      salt '*' chef.run key_file='/etc/chef/custom.pem'


  validation_key
    Specify where to find a validation.pem (for automatic client registration)

    CLI Example::
      salt '*' chef.run validation_key='/var/tmp/newchef.pem'


  server
    Overide the chef_server_url option in the Chef client.rb.

    CLI Example::
      salt '*' chef.run server='http://otherchef.example.com:4000'


  environment
    Specify the environment to run in.

    CLI Example::
      salt '*' chef.run environment=testing


  log_level
    Control the level of logging verbosity for this client run.

    CLI Example::
      salt '*' chef.run log_level=debug


  node_name
    Override the node name, either from client.rb or the automatically-detected
    FQDN.  This setting is important as it must match the user named in the
    client.pem or PEM file specified with key_file.  It is how this node will
    identify itself / authenticate with the Chef server.

    CLI Example::
      salt '*' chef.run node_name='awesome.example.com'


  dry_run
    Don't actually change anything, just log what would have happened.

    CLI Example::
      salt '*' chef.run dry_run=True

  '''

  cmd = _get_chef_client()

  if cmd is None:
    return "Unable to locate chef-client executable."

  tmpjson = None

  if force is False:
    skipfile = __salt__['config.option']('chef.client.skipfile', '/etc/chef/no_chef_run')

    if os.path.isfile(skipfile):
      return "Skipfile is present at %s, skipping" % skipfile

  if isinstance(run_list, list):
    tmpjson = tempfile.TemporaryFile()
    tmpjson.write('{"run_list": [%s]}' % ','.join(run_list))
    cmd += ' --json-attributes %s' % tmpjson.name

  if not key_file is None:
    if os.path.isfile(key_file):
      cmd += ' --client_key %s' % key_file

  if not validation_key is None:
    if os.path.isfile(validation_key):
      cmd += ' --validation_key %s' % validation_key

  if not server is None:
    cmd += ' --server %s' % server

  if not environment is None:
    cmd += ' --environment %s' % environment

  if not log_level is None:
    cmd += ' --log_level %s' % log_level

  if not node_name is None:
    cmd += ' --node-name %s' % node_name

  if not dry_run is None:
    cmd += ' --why-run'

# run the thing
  rv = __salt__['cmd.run'](cmd).split('\n')

# clean the thing
  if not tmpjson is None:
    tmpjson.close()

# return the thing
  return rv

