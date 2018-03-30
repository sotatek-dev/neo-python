"""
Example of running a NEO node and receiving notifications when events
of a specific smart contract happen.

Events include Runtime.Notify, Runtime.Log, Storage.*, Execution.Success
and several more. See the documentation here:

http://neo-python.readthedocs.io/en/latest/smartcontracts.html
"""
import threading
from time import sleep

from logzero import logger
from twisted.internet import reactor, task
from neocore.UInt160 import UInt160
from neocore.UInt256 import UInt256
from neocore.Cryptography.Crypto import Crypto

from neo.contrib.smartcontract import SmartContract
from neo.Network.NodeLeader import NodeLeader
from neo.Core.Blockchain import Blockchain
from neo.Implementations.Blockchains.LevelDB.LevelDBBlockchain import LevelDBBlockchain
from neo.Settings import settings
settings.setup_mainnet()
import mysql.connector

config = {
  'user': 'root',
  'password': '123456',
  'host': 'localhost',
  'database': 'CGE_transfer',
  'raise_on_warnings': True,
}

cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()
add_transfer = ("INSERT INTO transfer_event (txid, from_address, to_address, value, success) VALUES (%s, %s, %s, %s, %s)")
add_refund = ("INSERT INTO refund_event (txid, address, value, success) VALUES (%s, %s, %s, %s)")
# If you want the log messages to also be saved in a logfile, enable the
# next line. This configures a logfile with max 10 MB and 3 rotations:
settings.set_logfile(".log/cge-event.log", max_bytes=1e7, backup_count=3)

# Setup the smart contract instance
smart_contract = SmartContract("34579e4614ac1a7bd295372d3de8621770c76cdc")


# Register an event handler for Runtime.Notify events of the smart contract.
@smart_contract.on_notify
def sc_notify(event):
    logger.info("SmartContract Runtime.Notify event: %s", event)

    # Make sure that the event payload list has at least one element.
    if not len(event.event_payload):
        return

    # The event payload list has at least one element. As developer of the smart contract
    # you should know what data-type is in the bytes, and how to decode it. In this example,
    # it's just a string, so we decode it with utf-8:
    eventType = event.event_payload[0].decode("utf-8")
    if eventType == "transfer":
        from_addr = Crypto.ToAddress(UInt160(data=event.event_payload[1]))
        to_addr = Crypto.ToAddress(UInt160(data=event.event_payload[2]))
        value = int.from_bytes(event.event_payload[3], 'little')
        data_transfer = (str(event.tx_hash), from_addr, to_addr, value, event.execution_success)
        cursor.execute(add_transfer, data_transfer)
        cnx.commit()
        logger.info("[transfer]: %s %s %s %s %s", event.tx_hash, from_addr, to_addr, value, event.execution_success)
    if eventType == "refund":
        address = Crypto.ToAddress(UInt160(data=event.event_payload[1]))
        amount = event.event_payload[2]
        data_refund = (str(event.tx_hash), address, str(amount), event.execution_success)
        cursor.execute(add_refund, data_refund)
        cnx.commit()
        logger.info("[refund]: %s %s %s %s", event.tx_hash, address, amount, event.execution_success)
    logger.info("- payload part 1: %s", event.event_payload[0].decode("utf-8"))


def custom_background_code():
    """ Custom code run in a background thread. Prints the current block height.

    This function is run in a daemonized thread, which means it can be instantly killed at any
    moment, whenever the main thread quits. If you need more safety, don't use a  daemonized
    thread and handle exiting this thread in another way (eg. with signals and events).
    """
    while True:
        logger.info("Block %s / %s", str(Blockchain.Default().Height), str(Blockchain.Default().HeaderHeight))
        sleep(15)


def main():
    # Setup the blockchain
    blockchain = LevelDBBlockchain(settings.chain_leveldb_path)
    Blockchain.RegisterBlockchain(blockchain)
    try
        dbloop = task.LoopingCall(Blockchain.Default().PersistBlocks)
    except Exception as e:
        logger.info("Ignore error to continue: %s " % e)
    dbloop.start(.1)
    NodeLeader.Instance().Start()

    # Disable smart contract events for external smart contracts
    settings.set_log_smart_contract_events(False)

    # Start a thread with custom code
    d = threading.Thread(target=custom_background_code)
    d.setDaemon(True)  # daemonizing the thread will kill it when the main thread is quit
    d.start()

    # Run all the things (blocking call)
    logger.info("Everything setup and running. Waiting for events...")
    reactor.run()
    logger.info("Shutting down.")


if __name__ == "__main__":
    main()

