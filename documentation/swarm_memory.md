# Layer 2 Function

# Create Operation
Write a new value to the swarm memory.

## Current Implementation
1. Value is saved locally to the memory section of the swarm bot that writes the value.
2. Message is then propagated throughout the network with the object ID, holder ID, and data type for the object.

## Issues with the current implementation
- There is no load balancing. If a bot makes 100% of write requests, it will store 100% of the data defeating the purpose of having a swarm memory.
- No data redundancy. If a bot leaves the swarm, all of its data is lost.
- Need to propagate a message each time a value is written. This can create heavy strain on the network if a lot of writes are occuring simulataneously. 
- Each bot needs to maintain a record of all of the values in the swarm and where they are located. This would not scale well for bots with low memory.

## New proposed solution
The bot where information is stored in the swarm memory can be thoughout similar to a computer. For example, if it's stored locally on the bot's swarm memory allocation, that would be similar to the bot's cache. If the data is stored on a directly connected bot, that would be the RAM. Finally, if it's stored on a non-directly connected bot, that would be the hard drive.

1. Determine which bots will hold the new information. These shall be the bots with the current smallest load that also have enough remaining space to hold the value. This will help with load balancing and data redundancy.
2. Push out content discovery to read time. Instead of propagating a message at write time, bots will learn of content at read time. Bots will query the swarm memory by propagating a read query message and then receive contents IDs and location IDs. This will remove the need for propagation and bots will no longer have to maintain a full list of all swarm memory contents. Instead, they will just be responsible for tracking local contents.

### Current questions
1. How to measure and track load on each bot?
The load shall be a function of the memory on the machine. One implementation could be the percentage of used allocated swarm memory space. This shall be configurable using subclasses similar to propagation strategies.
2. How to track remaining space on each bot?
A ledger that is periodically synced shall be used. The ledger will be a map of the IDs of all bots connected to a given bot along with the current load on the bot. The sync strategy can then be set. Some potential strategies include time based or change percentage based. The ledger size will scale linearly with each newly connected bot, but may lead to pockets in the swarm memory.
3. How many bots should hold the information to account for redundancy?
Machine learning shall be used to place information optimally. This will minimize response time while maximizing security.

# Read Operation
Read a new value from the swarm memory.

## Current Implementation
1. Bot gets the ID of the holder for the given object ID.
2. 
- If the ID is not valid, then a value of None is returned.
- If the bot doing the read is holding the data, it is returned right away.
- If a different bot is holding the data, a direct async message is sent to the bot that is holding the data requesting a read.
    - If the holder bot is still holding the requested object, it sends a new message with the object key, value, and data type.
    - When the bot that made the original read request gets the final signal, it stores the value to its local memory.

## Issues with the current implementation
- Only works in a fully connected network. Since the reads require direct messages to transfer values, each bot must be conencted to every other bot in the network.
- Each bot needs to maintain a record of all of the values in the swarm and where they are located. This would not scale well for bots with low memory.

## New proposed solution
1. Bots create a query and propagate it throughout the network in an async manner
2. When bots receive the message, they parse and handle the message, sending a new message with the response if needed


# Use ML to optimize:
1. Location of information to ensure it is close to bots that need it
2. Number of copies of information
3. Load balancing of accesses, transfers, and memory use