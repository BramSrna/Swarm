# Layer 1
- Bots only know of bots they are directly connected to
- Bots exchange information using only asynchronous messages
- Swarm can be any amount of connected from a string formation to a fully connected swarm

# Layer 2
- Built on top of layer 1
- Simulates a fully connected swarm regardless of underlying connectivity
- Bots can exchange messages synchronously
- Increased collaboration among bots

# Building Layer 2
Layer 1 is built in the swarm_bot object that extends the network_node subclass.
- Layer 2 shall be able to call layer 1 functions, but layer 1 cannot call layer 2 functions
- Layer 2 could be the swarm_bot, while layer 1 is the network_node