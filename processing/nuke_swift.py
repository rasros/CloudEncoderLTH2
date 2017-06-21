#!/usr/bin/env python2
from control.openstack import WaspSwiftConn
import swiftclient

import sys

#taken from 
#https://stackoverflow.com/questions/3041986/apt-command-line-interface-like-yes-no-input
def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = raw_input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")

if __name__ == '__main__':
    if not query_yes_no("This will delete all video data! Are you sure you want to procede?"):
        return

    conf = WaspSwiftConn()
    conf.readConf()
    swift = conf.swiftConn()

    for container in swift.get_account()[1]:
        cname = container['name']
        for data in conn.get_container(cname)[1]:
            fname = data['name']
            swift.delete_object(cname, fname)
        swift.delete_container(cname)
    swift.close()
