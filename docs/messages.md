# Messages

Below is described the structure of the messages being sent and received by the system.
Each message starts with the same 5 bytes (one message type + four total length)
followed by the payload.
Message type is set to `0` when it can be deduced from the previous messages on that same connection stream.

## Read

Request from client to communicating node:

* message type, one byte, value `200`
* message total length, four big-endian bytes (value always `13`)
* key to be read, 8 big-endian bytes

Request from the communicating node to the leader node:

* message type, one byte, value `1`
* message total length, four big-endian bytes (value always `13`)
* key to be read, 8 big-endian bytes

Response from the leader node to the communicating node
and from there to the client:

* message type, one byte, value `0`
* message total length, four big-endian bytes
* the value


## Write

Request from the client to the communicating node:

* message type, one byte, value `202`
* message total length, four big-endian bytes (value always `13`)
* key to be written, 8 big-endian bytes

Request from the communicating node to the leader node:

* message type, one byte, value `2`
* message total length, four big-endian bytes (value always `13`)
* key to be written, 8 big-endian bytes

Response (write permission) from the leader node to the communicating node
and from there to the client:

* message type, one byte, value `0`
* message total length, four big-endian bytes
* the current value

The write command from the client to the communicating node
and from there to the leader node:

* message type, one byte, value `0`
* message total length, four big-endian bytes
* the new value

Final response (acknowledgement) from the leader node to the communicating node
and from there to the client:

* message type, one byte, value `0`
* message total length, four big-endian bytes (value always `7`)
* two bytes, value `[111, 107]`


## Backups

Request from the leader node to the neighbor to write a backup:

* message type, one byte, value `20`
* message total length, four big-endian bytes
* the key, 8 big-endian bytes
* the value

Acknowledgement response from the backup neighbor to the leader node:

* message type, one byte, value `0`
* message total length, four big-endian bytes (value always `7`)
* two constant bytes, `[111, 107]`

## Join

Request from the joining node to the one known node:

* message type, one byte, value `10`
* message total length, four big-endian bytes (value always `5`)

The response:

* message type, one byte, value `0`
* message total length, four big-endian bytes
* one or more of these 12-byte items:
    * ID of the node, 8 big-endian bytes
    * IP address of the node, 4 big-endian bytes

The list of nodes in the response does contain the responding known node itself
with IP address `127.0.0.1` and it is up to the joining node to replace that with something useful.
The joining node is not included in the list.



Request from the joining node to its greater neighbor
requesting the primary key-value pairs:

* message type, one byte, value `11`
* message total length, four big-endian bytes (value always `21`)
* inclusive lower bound of the IDs to transfer, 8 big-endian bytes
* inclusive upper bound of the IDs to transfer, 8 big-endian bytes

The response:

* message type, one byte, value `0`
* message total length, four big-endian bytes
* zero or more of these items:
    * the key, 8 big-endian bytes
    * value length, four big-endian bytes
    * the value



Request from the joining node to its neighbor
requesting the key-value pairs for backup:

* message type, one byte, value `12`
* message total length, four big-endian bytes (value always `5`)

Note that the key-value pairs are backups from the joining node's viewpoint,
but primary (leader) pairs from the receiver's viewpoint.

The response:

* message type, one byte, value `0`
* message total length, four big-endian bytes
* zero or more of these items:
    * the key, 8 big-endian bytes
    * value length, four big-endian bytes
    * the value



The announcement from the joining node to every other node:

* message type, one byte, value `13`
* message total length, four big-endian bytes (value always `13`)
* ID of the joining node, 8 big-endian bytes

The acknowledgement response from another node to the joining node:

* message type, one byte, value `0`
* message total length, four big-endian bytes (value always `7`)
* two constant bytes, `[111, 107]`


## Fault tolerance

The message to a node indicating that the receiver's neighbor is down:

* message type, one byte, value `30`
* message total length, four big-endian bytes (value always `13`)
* the ID of the crashed node, 8 big-endian bytes

Response to this message acknowledging that the fault is handled:

* message type, one byte, value `0`
* message total length, four big-endian bytes (value always `7`)
* two constant bytes, `[111, 107]`



The message from a node to everybody announcing that a certain node is down:

* message type, one byte, value `31`
* message total length, four big-endian bytes (value always `13`)
* the ID of the crashed node, 8 big-endian bytes

Response to this announcement:

* message type, one byte, value `0`
* message total length, four big-endian bytes (value always `7`)
* two constant bytes, `[111, 107]`
