import sys
import traceback
import atexit
import logging
import os.path
from code import compile_command

import click
from rssant_common.logger import configure_logging

from .client import BackdoorClient
from .helper import connect_first_available_server


HISTORY_PATH = os.path.expanduser("~/.backdoor-history")


def _save_history():
    import readline
    readline.write_history_file(HISTORY_PATH)


def _enable_completer(context=None):
    try:
        import readline
    except ImportError:
        return
    try:
        import rlcompleter
    except ImportError:
        return
    readline.set_completer(rlcompleter.Completer(context).complete)
    readline.parse_and_bind("tab:complete")
    # command history
    if os.path.exists(HISTORY_PATH):
        readline.read_history_file(HISTORY_PATH)
    atexit.register(_save_history)


class PythonInteractiveConsole:
    """Closely emulate the behavior of the interactive Python interpreter.

    This class builds on InteractiveInterpreter and adds prompting
    using the familiar sys.ps1 and sys.ps2, and input buffering.
    """

    def __init__(self):
        self.resetbuffer()

    def runsource(self, source: str):
        raise NotImplementedError

    def resetbuffer(self):
        """Reset the input buffer."""
        self.buffer = []

    def interact(self, banner=None, exitmsg=None):
        """Closely emulate the interactive Python console.

        The optional banner argument specifies the banner to print
        before the first interaction; by default it prints a banner
        similar to the one printed by the real Python interpreter,
        followed by the current class name in parentheses (so as not
        to confuse this with the real interpreter -- since it's so
        close!).

        The optional exitmsg argument specifies the exit message
        printed when exiting. Pass the empty string to suppress
        printing an exit message. If exitmsg is not given or None,
        a default message is printed.

        """
        try:
            sys.ps1
        except AttributeError:
            sys.ps1 = ">>> "
        try:
            sys.ps2
        except AttributeError:
            sys.ps2 = "... "
        cprt = 'Type "help", "copyright", "credits" or "license" for more information.'
        if banner is None:
            self.write("Python %s on %s\n%s\n(%s)\n" %
                       (sys.version, sys.platform, cprt,
                        self.__class__.__name__))
        elif banner:
            self.write("%s\n" % str(banner))
        more = 0
        while 1:
            try:
                if more:
                    prompt = sys.ps2
                else:
                    prompt = sys.ps1
                try:
                    line = self.raw_input(prompt)
                except EOFError:
                    self.write("\n")
                    break
                else:
                    more = self.push(line)
            except KeyboardInterrupt:
                self.write("\nKeyboardInterrupt\n")
                self.resetbuffer()
                more = 0
        if exitmsg is None:
            self.write('now exiting %s...\n' % self.__class__.__name__)
        elif exitmsg != '':
            self.write('%s\n' % exitmsg)

    def push(self, line):
        """Push a line to the interpreter.

        The line should not have a trailing newline; it may have
        internal newlines.  The line is appended to a buffer and the
        interpreter's runsource() method is called with the
        concatenated contents of the buffer as source.  If this
        indicates that the command was executed or invalid, the buffer
        is reset; otherwise, the command is incomplete, and the buffer
        is left as it was after the line was appended.  The return
        value is 1 if more input is required, 0 if the line was dealt
        with in some way (this is the same as runsource()).

        """
        self.buffer.append(line)
        source = "\n".join(self.buffer)
        more = self.runsource(source)
        if not more:
            self.resetbuffer()
        return more

    def raw_input(self, prompt=""):
        """Write a prompt and read a line.

        The returned line does not include the trailing newline.
        When the user enters the EOF key sequence, EOFError is raised.

        The base implementation uses the built-in function
        input(); a subclass may replace this with a different
        implementation.

        """
        return input(prompt)

    def write(self, data):
        """Write a string.

        The base implementation writes to sys.stderr; a subclass may
        replace this with a different implementation.

        """
        sys.stderr.write(data)


class BackdoorShell(PythonInteractiveConsole):
    def __init__(self, pid, sock=None):
        super().__init__()
        self.pid = pid
        self.client = BackdoorClient(pid, sock=sock)
        info_response = self.client.request('info')
        assert info_response.ok, 'failed to get backdoor server info!'
        self.info = dict(info_response.content)

    def interact(self, banner=None, exitmsg=None):
        _enable_completer()
        if banner is None:
            total_memory_usage = self.info['total_memory_usage'] // 1024 // 1024
            banner = "Backdoor PID={} threads={} memory={}M Python {} on {}".format(
                self.pid, self.info['num_active_threads'], total_memory_usage,
                self.info['version'], self.info['platform'])
        if exitmsg is None:
            exitmsg = ''
        super().interact(banner=banner, exitmsg=exitmsg)

    def runsource(self, source):
        if not source:
            return False
        try:
            code_obj = compile_command(source)
        except (SyntaxError, ValueError, OverflowError):
            traceback.print_exc()
            return False
        if code_obj is None:
            return True
        response = self.client.request('eval', source=source)
        if response.content:
            self.write(response.content)
        return False


@click.command("Backdoor Shell")
@click.option('-p', '--pid', help="Server PID")
@click.option('-v', '--verbose', is_flag=True, help="Show debug logs")
def main(pid=None, verbose=False):
    level = logging.DEBUG if verbose else logging.INFO
    configure_logging(level=level)
    sock = None
    if pid is None:
        pid, sock = connect_first_available_server()
    if pid is None:
        raise click.BadOptionUsage('pid', "Server PID is required!")
    shell = BackdoorShell(pid, sock=sock)
    shell.interact()


if __name__ == "__main__":
    main()
