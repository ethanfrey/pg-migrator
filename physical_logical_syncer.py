import argparse
import time
import sys

from receiver import PsycopgReceiver


def main(source, dest, slot, trigger):
    # TODO: load migrations from external file???
    master = PsycopgReceiver(slot=slot, **source)
    slave = PsycopgReceiver(slot=slot, **dest)

    if not slave.is_slave():
        raise Exception("Target is already promoted, no luck using physical replication")

    master.create_replication_slot()
    logical_start = master.get_slot_location()
    print "Created slot '{}' at '{}'".format(slot, logical_start)

    slave_state = slave.check_physical_xlog_replay()
    while slave_state < logical_start:
        print "Waiting for slave to catch up: {} < {}".format(slave_state, logical_start)
        time.sleep(3)
        slave_state = slave.check_physical_xlog_replay()

    print "Slave is in a good state: {}".format(slave_state)
    print "Promoting with trigger file: {}...".format(trigger)
    # docker exec pg_slave gosu postgres pg_ctl promote -D /var/lib/postgresql/data
    slave.promote_slave(trigger)

    timeout = 30
    while slave.is_slave():
        timeout -= 1
        if timeout < 0:
            print "** ERROR: slave not promoted - aborting **"
            import pdb
            pdb.set_trace()
        else:
            sys.stdout.write('.')
            time.sleep(1)
    sys.stdout.write('\n')

    slave_state = slave.check_physical_xlog_replay()
    print "Eating changes up to {} on the master...".format(slave_state)
    master.consume_slot_changes(slave_state)

    print ""
    print "**** Switch to logical replication completed ******"
    print "Using slot: {}".format(slot)
    print "Time to run the migration by hand and start the logical replication script."
    print "Have fun! :)"


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Logical replication with optional migrations.')
    parser.add_argument('--src_db',
                        required=True,
                        help='databse name to replicate from')
    parser.add_argument('--dest_db',
                        required=True,
                        help='databse name to replicate to')
    parser.add_argument('--src_port',
                        help='databse name to replicate from')
    parser.add_argument('--dest_port',
                        help='databse name to replicate to')
    parser.add_argument('--src_host',
                        help='databse host to replicate from')
    parser.add_argument('--dest_host',
                        help='databse host to replicate to')
    parser.add_argument('--src_user',
                        help='databse user to authenticate')
    parser.add_argument('--dest_user',
                        help='databse user to authenticate')
    parser.add_argument('--src_password',
                        help='databse password to authenticate')
    parser.add_argument('--dest_password',
                        help='databse password to authenticate')
    parser.add_argument('--slot',
                        required=True,
                        help='replication slot name')
    parser.add_argument('--trigger',
                        required=True,
                        help='name of the trigger file (on slave filesystem)')

    args = parser.parse_args()
    source = dict(database=args.src_db, port=args.src_port, host=args.src_host, user=args.src_user, password=args.src_password)
    dest = dict(database=args.dest_db, port=args.dest_port, host=args.dest_host, user=args.dest_user, password=args.dest_password)
    main(source=source, dest=dest, slot=args.slot, trigger=args.trigger)
