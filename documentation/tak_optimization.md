# Task Optimization

## Basic Steps
1. Task bundle is received by swarm and execution group is formed (for now, let's say the bundle only has one task and so there is only one bot in the execution group)
2. Bot initializes task contest alongside a simulator
3. As the bot performs task execution iterations, it iterates on its internal simulator
4. Once the simulator is mature enough, the bot can use the simulator to iterate on the task

## Testing
To test this out:
- Create a simulator of some system (for example, a task to move blocks from point A to point B)
- Give a bot a task to execute in the simulator
- After X iterations compare the simulator created by the bot to the simulator used for testing, where X is the simulator maturation iteration
- After 2X iterations compare the new task created by the swarm bot to the original task to see if it is better

## Optimizing In Execution Groups With Multiple Bots
Expanding on the above for situations for bundles with multiple of the same task type:
- Bots in the same execution group, executing the same task type, shall use blockchain federated learning to iterate on the simulator
  - Bots will validate new simulators using their own internally stored validation information
- When iterating on tasks, blockchain based federated learning will also be used, but proof of simulation will be used to validate new task iterations

### Handling Mixed Task Type Execution Groups
For mixed bundles:
- Simulator shall be shared accross all tasks in the execution group
- Task iteration shall only be iterated amonst other bots in the execution group with the same task type
- Larger scope level of monitoring shall be maintained to see if new tasks should be created or tasks can be inter-optimized
