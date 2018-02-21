#!/usr/bin/env python
"""CLI for running NitFix functions."""

# pylint: disable=no-self-use,unused-argument

import cmd
import glob
import textwrap
from lib.data import master_taxonomy


class NitfixShell(cmd.Cmd):
    """Define an interpreter for running nitfix functions."""

    intro = 'Run NitFix commands. Type help or ? to list commands \n'
    prompt = 'nitfix> '

    sheets = ['taxonomy', 'samples']

    def do_import(self, line):
        """Import the Master Taxonomy Google sheet into a CSV file."""
        master_taxonomy.import_sheet(line)

    def complete_import(self, text, line, begidx, endidx):
        """Autocompletion for the import command."""
        if len(line.split()) < 3:
            return self.option_complete(text, self.sheets)
        return self.file_name_complete(text)

    def help_import(self):
        """Help for the import command."""
        print(textwrap.dedent("""
            Import the Master Taxonomy Google sheet into a CSV file.

            import SHEET PATH

            SHEET: The Google Sheet to import. Options are: (taxonomy, samples)
            PATH: The complete path of where to write the CSV file.
        """))

    def do_eof(self, line):
        """Cleanup and exit."""
        print('Exiting')
        return True

    def option_complete(self, text, options):
        """Autocomplete options list."""
        if not text:
            return options
        return [opt for opt in options if opt.startswith(text)]

    def file_name_complete(self, text):
        """Autocomplete file name."""
        print(text)
        print(glob.glob(text + '*'))
        return glob.glob(text + '*')

    def precmd(self, line):
        """Preprocess the commands."""
        line = line.lower()
        return line


if __name__ == '__main__':
    NitfixShell().cmdloop()
