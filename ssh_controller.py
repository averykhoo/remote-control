import datetime
import json
import os
import time
import warnings

import pandas as pd
import paramiko


class SSHConnection:

    def __init__(self, ip_address, port, username, password, timeout=None):
        self.ip_address = ip_address
        self.port = port
        self.username = username
        self.password = password
        self.ssh_conn = None
        self.timeout = timeout

    def __enter__(self):
        self.ssh_conn = paramiko.SSHClient()
        self.ssh_conn.load_system_host_keys()
        self.ssh_conn.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        self.ssh_conn.connect(hostname=self.ip_address,
                              port=self.port,
                              username=self.username,
                              password=self.password,
                              timeout=self.timeout)
        return self.ssh_conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.ssh_conn is not None:
            self.ssh_conn.close()
            self.ssh_conn = None


class SSH:
    def __init__(self, ip_address, port, username, password, logfile='ssh.log', name=None):
        self.ip_address = ip_address
        self.port = port
        self.username = username
        self.password = password
        self.logfile = logfile
        self.name = name
        self.log_separator = '--'  # compatible with jdump files

        try:
            with SSHConnection(self.ip_address, self.port, self.username, self.password, timeout=30):
                pass
        except:
            print('SSH connection test failed')
            raise

        self._log({'function': 'init'})

    def __str__(self):
        insert_name = f'[{self.name}]=' if self.name is not None else ''
        return f'SSH<{insert_name}{self.username}@{self.ip_address}:{self.port}>'

    def _log(self, json_data):

        json_data['ip_address'] = self.ip_address
        json_data['port'] = self.port
        json_data['username'] = self.username
        json_data['timestamp'] = datetime.datetime.now().isoformat()

        if self.logfile is not None:
            for _ in range(5):
                try:
                    with open(self.logfile, mode='at', encoding='utf8', newline='\n') as f:
                        f.write(json.dumps(json_data, indent=4, sort_keys=True, ensure_ascii=False) + '\n')
                        if self.log_separator:
                            f.write(self.log_separator + '\n')
                    break
                except:
                    time.sleep(1)

    def execute(self, command, wait_for_output=True):
        out = None
        err = None

        self._log({'function': 'execute',
                   'command':  command,
                   })

        # run command and (maybe) get output
        with SSHConnection(self.ip_address, self.port, self.username, self.password) as ssh_conn:
            stdin, stdout, stderr = ssh_conn.exec_command(command)

            if wait_for_output:
                out = stdout.read()

                try:
                    out = out.decode('utf8')
                except:
                    print('could not decode stdout as utf8')

                try:
                    err = stderr.read().rstrip()
                except:
                    print('could not read stderr')
                    err = b''

                try:
                    err = err.decode('utf8')
                except:
                    print('could not decode stderr as utf8')

            elif 'nohup' not in command:
                print('usage of `nohup` recommended for long-running commands')

        # warn on error
        if err:
            warnings.warn(err)

        # return output
        return out

    def kill(self, pid: int):
        pid = int(pid)
        assert pid > 10  # don't kill the kernel pls

        self._log({'function': 'kill',
                   'pid':      pid,
                   })

        self.execute(f'kill -9 {pid}')

    def ps_ef(self, cmd_grep_patterns=None, kill=False):
        headers = ['User', 'PID', 'Parent PID', 'CPU%', 'Start Time', 'TTY', 'Running Time', 'Command']

        if cmd_grep_patterns is None:
            cmd_grep_patterns = []
        elif type(cmd_grep_patterns) is str:
            cmd_grep_patterns = [cmd_grep_patterns]

        self._log({'function':          'ps_ef',
                   'cmd_grep_patterns': cmd_grep_patterns,
                   })

        # get ps info
        lines = [line.split(maxsplit=7) for line in self.execute('ps -ef').split('\n')[1:] if line.strip()]
        df = pd.DataFrame(lines, columns=headers)
        df['PID'] = df['PID'].apply(lambda pid: int(pid))
        df['Parent PID'] = df['Parent PID'].apply(lambda pid: int(pid))
        df['CPU%'] = df['CPU%'].apply(lambda c: int(c))

        # filter to desired rows
        for pattern in cmd_grep_patterns:
            df = df[df['Command'].str.contains(pattern)]

        # nothing to kill
        if not kill:
            return df

        # not filtered, so don't kill
        if not cmd_grep_patterns:
            warnings.warn('not allowed to kill all processes, please specify a command grep pattern')
            return df

        # what to kill
        pids_to_kill = sorted(df['PID'].unique())

        # invalid kill target
        if any(pid <= 10 for pid in pids_to_kill):
            warnings.warn('not allowed to kill pid <= 10')
            return df

        # kill the things
        for i, pid in enumerate(pids_to_kill):
            print(f'[{i + 1}/{len(pids_to_kill)}] killing process with PID={pid}')
            self.kill(pid)

        # done
        return df

    def process_running(self, process_name, cmd_grep_patterns, grep_case=True):
        if cmd_grep_patterns is None:
            cmd_grep_patterns = []
        elif type(cmd_grep_patterns) is str:
            cmd_grep_patterns = [cmd_grep_patterns]

        self._log({'function':          'process_running',
                   'remote_path':       process_name,
                   'cmd_grep_patterns': cmd_grep_patterns,
                   })

        cmd = 'ps -ef'
        for pattern in cmd_grep_patterns:
            if grep_case:
                cmd += f' | grep "{pattern}"'
            else:
                cmd += f' | grep -i "{pattern}"'

        return process_name in self.execute(cmd)

    def exists(self, remote_path):
        remote_path = str(remote_path)
        assert remote_path.startswith('/')

        self._log({'function':    'exists',
                   'remote_path': remote_path,
                   })

        if self.execute(f'ls -l {remote_path}').rstrip('\r\n'):
            return True
        return False

    def mkdir(self, remote_path, parents=True):
        remote_path = str(remote_path)
        assert remote_path.startswith('/'), 'remote path must be absolute'

        self._log({'function':    'mkdir',
                   'remote_path': remote_path,
                   })

        if parents:
            return self.execute(f'mkdir --parents "{remote_path}"')
        else:
            return self.execute(f'mkdir "{remote_path}"')

    def mv(self, remote_path, new_remote_path):
        remote_path = str(remote_path)
        assert remote_path.startswith('/')

        self._log({'function':    'mv',
                   'remote_path': remote_path,
                   })

        return self.execute(f'mv "{remote_path}" "{new_remote_path}"')

    def rm(self, remote_path, recursive=False, force=True):
        remote_path = str(remote_path)
        assert remote_path.startswith('/')
        assert remote_path.count('/') > 1  # don't delete root pls

        self._log({'function':    'tar_gz',
                   'remote_path': remote_path,
                   })

        rm_command = 'rm '

        if recursive and force:
            rm_command += '-rf '

        elif recursive:
            rm_command += '-r '

        elif force:
            rm_command += '-f '

        rm_command += f'"{remote_path}"'

        return self.execute(rm_command)

    def tar_gz(self, remote_target, remote_output_path):
        remote_target = str(remote_target)
        remote_output_path = str(remote_output_path)
        self._log({'function':           'tar_gz',
                   'remote_target':      remote_target,
                   'remote_output_path': remote_output_path,
                   })

        # must be using absolute paths
        assert remote_target.startswith('/')
        assert remote_output_path.startswith('/')
        assert remote_output_path.endswith('.tgz') or remote_output_path.endswith('.tar.gz')

        # source exists
        assert self.exists(remote_target)

        # temp path
        tmp_path = remote_output_path + '.partial'

        # tar and gz the stuff
        ret = self.execute(f'cd "{os.path.dirname(remote_target)}"; '
                           f'tar cvzf "{tmp_path}" "{os.path.basename(remote_target)}"')

        # verbose
        print(ret)

        # rename the temp file
        if self.exists(tmp_path):
            if self.exists(remote_output_path):
                self.rm(remote_output_path)
            self.mv(tmp_path, remote_output_path)
            return remote_output_path

    def scp_remote_to_local(self, remote_path, local_path, overwrite=False, verbose=True):
        remote_path = str(remote_path)
        local_path = os.path.abspath(local_path)

        # must use absolute path for remote
        assert remote_path.startswith('/')

        # don't overwrite?
        if os.path.exists(local_path) and not overwrite:
            print(f'overwrite is disabled and local path exists: <{local_path}>')
            return

        # source exists
        assert self.exists(remote_path)

        # temp path
        tmp_path = local_path + '.partial'
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

        # make dir
        if not os.path.isdir(os.path.dirname(tmp_path)):
            assert not os.path.exists(os.path.dirname(tmp_path))
            os.makedirs(os.path.dirname(tmp_path))

        # log
        if verbose:
            print(f'retrieving: <{remote_path}>')
            print(f'        to: <{local_path}>')
        self._log({'function':          'scp_remote_to_local',
                   'remote_source':     remote_path,
                   'local_destination': local_path,
                   })

        # scp to temp path
        with SSHConnection(self.ip_address, self.port, self.username, self.password) as ssh_conn:
            ftp_conn = ssh_conn.open_sftp()
            try:
                ftp_conn.get(remote_path, tmp_path)
            except:
                print('could not retrieve file')

            ftp_conn.close()

        # rename and return if scp succeeded
        if os.path.exists(tmp_path):
            if os.path.exists(local_path):
                os.remove(local_path)
            os.rename(tmp_path, local_path)
            return local_path

    def scp_local_to_remote(self, local_path, remote_path, overwrite=False, verbose=True):
        remote_path = str(remote_path)
        local_path = os.path.abspath(local_path)

        # must use absolute path for remote
        assert remote_path.startswith('/')

        # don't overwrite?
        if self.exists(remote_path) and not overwrite:
            print(f'overwrite is disabled and remote path exists: <{remote_path}>')
            return

        # source exists
        assert os.path.exists(local_path)

        # temp path
        tmp_path = remote_path + '.partial'
        if self.exists(tmp_path):
            self.rm(tmp_path)

        # make dir
        if os.path.dirname(tmp_path):
            if not self.exists(os.path.dirname(tmp_path)):
                print(f'remote dir <{os.path.dirname(tmp_path)}> does not exist, creating...')
                self.mkdir(os.path.dirname(tmp_path))

        # log
        if verbose:
            print(f'transmitting: <{local_path}>')
            print(f'          to: <{remote_path}>')
        self._log({'function':           'scp_local_to_remote',
                   'local_source':       local_path,
                   'remote_destination': remote_path,
                   })

        # scp to temp path
        with SSHConnection(self.ip_address, self.port, self.username, self.password) as ssh_conn:
            ftp_conn = ssh_conn.open_sftp()
            try:
                ftp_conn.put(local_path, tmp_path)
            except:
                print('could not transmit file')

            ftp_conn.close()

        # rename and return if scp succeeded
        if self.exists(tmp_path):
            if self.exists(remote_path):
                self.rm(remote_path)
            self.mv(tmp_path, remote_path)
            return remote_path
