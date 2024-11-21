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
