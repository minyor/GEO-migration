import os
import subprocess
import tempfile
import time


class ShellRunner:
    shell_runner_filename = "/tmp/shell_runner_run.sh"
    shell_runner_script = "cmd_file=\"$1\"\n" \
                          "for (( ; ; ))\n" \
                          "do\n" \
                          "    if [ -e \"$cmd_file\" ]\n" \
                          "    then\n" \
                          "        bash \"$cmd_file\"\n" \
                          "        rm \"$cmd_file\"\n" \
                          "    fi\n" \
                          "done\n"

    def __init__(self, folder_path):
        text_file = open(ShellRunner.shell_runner_filename, "w")
        text_file.write(ShellRunner.shell_runner_script)
        text_file.close()

        self.folder_path = folder_path
        self.cmd_file_path = os.path.join(folder_path, "shell_runner_cmd.sh")
        self.shell_proc = subprocess.Popen(["bash", ShellRunner.shell_runner_filename, self.cmd_file_path])

    def run(self, cmd):
        text_file = open(self.cmd_file_path, "w")
        text_file.write(cmd)
        text_file.close()
        while os.path.isfile(self.cmd_file_path):
            time.sleep(0.1)

    @staticmethod
    def clean():
        proc = subprocess.Popen(['pkill', '-f', "bash " + ShellRunner.shell_runner_filename])
        time.sleep(1)
