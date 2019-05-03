#   SSH and RabbitMQ remote controls
*   simplify working with remote servers via ssh
*   simplify working with rabbitmq

##  rmq_controller.RMQ
*   `rmq = RMQ(ip_address, port, virtual_host, username, password, name=None)`
*   `str(rmq)`
*   `RMQ.get_count(queue_name)`
*   `RMQ.purge(queue_name)`
*   `RMQ.read_jsons(queue_name, n=-1, auto_ack=False)`
    *   if `n` < 0, reads *all* messages in queue
        *   otherwise, reads `n` messages from queue
    *   if `auto_ack` is set, acknowledges (and removes) messages from queue once read
*   `RMQ.write_jsons(queue_name, json_iterator)`

##  ssh_controller.SSH
*   `ssh = SSH(ip_address, port, username, password, logfile='ssh.log', name=None)`
*   `str(ssh)`
    *   if `logfile` is `None`, does not log output
*   `ssh.execute(self, command, wait_for_output=True)`
    *   if `wait_for_output` is set, blocks until command has completed and returns output
        *   otherwise, returns immediately
*   `ssh.kill(pid)`
*   `ssh.ps_ef(cmd_grep=None, kill_9=False)`
    *   if `cmd_grep_pattern` is provided, lists only rows matching the grep pattern
        *   otherwise, returns *all* rows (often more than 1k)
    *   if `kill_9` is set, kills all processes in resulting df
*   `ssh.process_running(process_name, cmd_grep_pattern)`
    *   `return process_name in self.execute(f'ps -ef | grep {cmd_grep_pattern}')`
*   `ssh.exists(remote_path)`
*   `ssh.mkdir(remote_path)`
*   `ssh.mv(remote_path, new_remote_path)`
*   `ssh.rm(remote_path, recursive=False, force=True)`
*   `ssh.tar_gz(remote_target, remote_output_path)`
*   `ssh.scp_remote_to_local(remote_path, local_path, overwrite=False)`
*   `ssh.scp_local_to_remote(local_path, remote_path, overwrite=False)`


## to-do
*   class verbose, method overwrite (default none)
*   rename queue_name since it takes multiple queues
*   update use of n in read_json