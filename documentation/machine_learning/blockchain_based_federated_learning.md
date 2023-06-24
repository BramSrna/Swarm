# Links
- https://en.wikipedia.org/wiki/Federated_learning
- https://arxiv.org/pdf/2110.02182.pdf
- https://arxiv.org/pdf/2010.01264.pdf

# Terminologies
- Clients: devices that work in FL system to collect data and train local models.
- Nodes: members in blockchain network to provide computing powers and generate new blocks, which can also be called miners.
- Aggregator: server or other powerful enough equipments to aggregate the global model.
- Distributed ledger: a traceable and audible database distributed across multiple nodes in blockchain network, storing data for retrieve or audit.
- Transaction: data records in each block.
- Local Model updates: gradients and weights computed by clients based on local raw data.

# Basic Federated Learning: Iterative Learning
1. Initialization: according to the server inputs, a machine learning model (e.g., linear regression, neural network, boosting) is chosen to be trained on local nodes and initialized. Then, nodes are activated and wait for the central server to give the calculation tasks.
2. Client selection: a fraction of local nodes is selected to start training on local data. The selected nodes acquire the current statistical model while the others wait for the next federated round.
3. Configuration: the central server orders selected nodes to undergo training of the model on their local data in a pre-specified fashion (e.g., for some mini-batch updates of gradient descent).
4. Reporting: each selected node sends its local model to the server for aggregation. The central server aggregates the received models and sends back the model updates to the nodes. It also handles failures for disconnected nodes or lost model updates. The next federated round is started returning to the client selection phase.
5. Termination: once a pre-defined termination criterion is met (e.g., a maximum number of iterations is reached or the model accuracy is greater than a threshold) the central server aggregates the updates and finalizes the global model.

# BCFL Types
## Fully Coupled BCFL
- Clients collect data and train the models locally.
- Local model updates are verified by the (selected) clients.
- Verified local updates are collected by (selected) clients and then the global model will be updated.
- New block which stores the verified model updates is added into the distributed ledger.
- According to incentive mechanism, rewards will be distributed to participates

### Example Fully Coupled BCFL Implementation
- Client collects data. Every set amount of data points, it splits the data into training, test, and validation data sets. New local model is trained on training dataset. If the new model hits a target threshold on the test dataset, the new local model is forwarded alongside the validation dataset. If the new model is beneath the threshold, the datasets are dropped.
- Once a local model has been created by a client, the client shall forward it to all Node bots along with the validation. The node bots check again that the model hits the desired accuracy on the validation dataset. Once revalidated, the model is added to the transaction pool along with the validation data.
- Aggregators compete to mine block. Once aggregator is determined, it aggregates the local model from the transaction pool and creates a new block with the aggregated model and all of the validation data.

## Flexibly Coupled BCFL
- Clients collect local data and train the local models, and then upload the local model updates to blockchain.
- Nodes on blockchain perform verification mechanism and only the validated updates can be used to update the global model.
- After the global model is aggregated, all the data will be stored on distributed ledger.
- Rewards are allocated to participates according to their performances

## Loosely Coupled BCFL
- Clients train models locally and upload the local model updates to blockchain.
- Nodes verify the local model updates, then generate reputation opinions for the clients.
- Nodes compete to generate new block which contains the reputation related data, and the new block will be added into the distributed ledger.
- Aggregator collects the verified updates and then execute the global model aggregation algorithm.
- Rewards and penalties are depended on the reputation opinions of clients

# Implementation In Current Swarm Architecture
The current swarm architecture has two features that can simplify the structure: swarm memory and swarm tasks. Normally, in a fully coupled BCFL, clients collect data and train the models, nodes validate models, and then the aggregators collect the validated models and train the combined model. Instead of having designated bots to act as the clients, nodes, and aggregators, the nodes and aggregators can be chosen on the fly while clients can be designated and undesignated as needed:
1. Initialize new federated learning task bundle. This defines the model, how data is collected, how the model is trained, the maximum number of clients (minimum is 1), and any additional configuration information needed.
2. Execution group is formed which definines the clients. Clients start executing collection + training process. When clients have a new model, they push the new model and the validation data to swarm memory. If the model threshold is hit, the client defines an aggregation task that will collect the models, reevaluate them, and retrain the model. The new task will be added to the pool.
3. The bot that receives the aggregation task will grab the models from the pool in swarm memory, clear the pool, aggregate the models, and push the new model to swarm memory. The clients shall then grab the new model from swarm memory.
4. The training, reevaluation process is then run until completion and the finished model is returned.

## Task Bundle Structure
Due to the nature of federated learning in a swarm structure, the task bundle can shift depending on the circustance and stage of the training. Due to this, a different configuration should be used to handle models requiring training. The training process shall still make use of the task execution functionality, but the priority will change based on the model lifecycle:
1. User defines the model and training information/configuration
2. Model gets added to swarm memory along with training information
3. Model produces tasks based on its lifecycle that get added to the task pool
