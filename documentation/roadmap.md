# Roadmap To 1.0
Currently, this project supports a rough swarm system. It has the infrastructure to support setting up a swarm by simulating fully connected nodes regardless of the underlying connections and sending messages between nodes in the swarm. In addition to this, the swarm supports a distributed database and a task execution engine. As of https://github.com/BramSrna/Swarm/pull/27, the swarm can support federated learning. This lays out the groundwork for the core functionality needed for swarm robotics. 

The remaining work for getting to 1.0 is primarily in the areas of handling load (swarm memory, task engine), scaling (horizontal and vertical), and test coverage. In addition to that work, there's also work related to dog fooding the task and ML functionality by incorporating it into the kernel.

# Handling Load
Need to expand the test coverage in the following areas:
- Swarm memory
- Task engine
- Swarm maintenance (bots being added and removed from the swarm)
- - Ensure that swarm syncing is handled properly for new bots and integrity is maintained when bots leave the swarm

# Scaling
Need to improve:
- Horizontal scaling
- - Ensure adding bots to the swarm actually helps improve the swarm
- - Minimize negative impact of adding new bots (memory impact and performance impact)
- Vertical scaling
- - Bots will have different capabilities in terms of the hardware they are running - the swarm should take this into account

# Test Coverage
- Unit test coverage
- E2E test coverage

# Task Engine Dog Fooding
- Add tasks to swarm maintenance
- Add tasks to the swarm memory
- Add tasks to task engine

# ML Functionality In Kernel
Look for areas where it can be integrated into the kernel. Some potential areas include:
- 