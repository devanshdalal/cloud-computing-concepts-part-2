/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

void PrintRing(std::vector<Node> ring)
{
	std::cout << "RING:";
	for (auto &node : ring)
	{
		std::cout << " " << node.getAddress()->getAddress();
	}
	std::cout << "\n";
}

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *address)
{
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	secondary = new HashTable();
	tertiary = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node()
{
	delete ht;
	delete secondary;
	delete tertiary;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing()
{
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	// bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());

	int curr_id = -1;
	for (int i = 0; i < ring.size(); ++i)
	{
		if (*(ring[i].getAddress()) == memberNode->addr)
		{
			curr_id = i;
			break;
		}
	}

	int new_id = -1;
	for (int i = 0; i < curMemList.size(); ++i)
	{
		if (*(curMemList[i].getAddress()) == memberNode->addr)
		{
			new_id = i;
			break;
		}
	}

	bool may_rehash = false;
	if (ring.size() >= 3)
	{
		for (int prev = -3; prev < 3; ++prev)
		{
			int safe_id = (curr_id + prev + (int)ring.size()) % ring.size();
			int safe_new_id = (new_id + prev + (int)curMemList.size()) % curMemList.size();
			if (*(curMemList[safe_new_id].getAddress()) == *(ring[safe_id].getAddress()))
			{
			}
			else
			{
				may_rehash = true;
				break;
			}
		}
	}
	ring = curMemList;

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	if (may_rehash)
	{
		std::cout << "Running stabilizationProtocol on " << memberNode->addr.getAddress() << "\n";
		stabilizationProtocol();
	}
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList()
{
	unsigned int i;
	vector<Node> curMemList;
	for (i = 0; i < this->memberNode->memberList.size(); i++)
	{
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key)
{
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret % RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE/UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreateOrUpdate(string key, string value, MessageType type)
{
	/*
	 * Implement this
	 */
	int current = g_transID++;
	auto nodes = findNodes(key);
	// std::cout << "Start clientCreate " << key << " " << nodes.size() << "\n";
	for (auto node_id = 0; node_id < nodes.size(); ++node_id)
	{
		auto req = Message(current, memberNode->addr, type, key, value,
						   static_cast<ReplicaType>(node_id));
		emulNet->ENsend(&(memberNode->addr), nodes[node_id].getAddress(), req.toString());
	}
	local_state_[current] = {type, key, value, par->getcurrtime() + WRITE_TIMEOUT, 0};
	// std::cout << "End clientCreate\n";
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value)
{
	clientCreateOrUpdate(key, value, MessageType::CREATE);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key)
{
	/*
	 * Implement this
	 */
	int current = g_transID++;
	std::cout << "Start clientRead " << memberNode->addr.getAddress() << " ";
	PrintRing(ring);
	auto nodes = findNodes(key);
	for (auto node_id = 0; node_id < nodes.size(); ++node_id)
	{
		std::cout << "clientRead on " << nodes[node_id].getAddress()->getAddress() << " for " << key << "\n";
		auto req = Message(current, memberNode->addr, MessageType::READ, key);
		emulNet->ENsend(&(memberNode->addr), nodes[node_id].getAddress(), req.toString());
	}
	local_state_[current] = {MessageType::READ, key, "", par->getcurrtime() + READ_TIMEOUT, 0};
	// std::cout << "End clientRead\n";
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value)
{
	/*
	 * Implement this
	 */
	clientCreateOrUpdate(key, value, MessageType::UPDATE);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key)
{
	int current = g_transID++;
	// std::cout << "Start clientDelete " << key << " " << current << "\n";
	auto nodes = findNodes(key);
	for (auto node_id = 0; node_id < nodes.size(); ++node_id)
	{
		auto req = Message(current, memberNode->addr, MessageType::DELETE, key);
		emulNet->ENsend(&(memberNode->addr), nodes[node_id].getAddress(), req.toString());
	}
	local_state_[current] = {MessageType::DELETE, key, "", par->getcurrtime() + WRITE_TIMEOUT, 0};
	// std::cout << "End clientDelete\n";
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica)
{
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table

	switch (replica)
	{
	case ReplicaType::PRIMARY:
	{
		ht->create(key, value);
		break;
	}
	case ReplicaType::SECONDARY:
	{
		secondary->create(key, value);
		break;
	}
	case ReplicaType::TERTIARY:
	{
		tertiary->create(key, value);
		break;
	}
	default:
		break;
	}
	return true;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key)
{
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	if (ht->count(key))
	{
		return ht->read(key);
	}
	if (secondary->count(key))
	{
		return secondary->read(key);
	}
	return tertiary->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica)
{
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
	switch (replica)
	{
	case ReplicaType::PRIMARY:
	{
		return ht->update(key, value);
	}
	case ReplicaType::SECONDARY:
	{
		return secondary->update(key, value);
	}
	case ReplicaType::TERTIARY:
	{
		return tertiary->update(key, value);
	}
	default:
		break;
	}
	return false;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key)
{
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
	return ht->deleteKey(key) || secondary->deleteKey(key) || tertiary->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages()
{
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char *data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while (!memberNode->mp2q.empty())
	{
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */
		// std::cout << "message:" << message << "\n";
		Message msg(message);
		// {CREATE, READ, UPDATE, DELETE, REPLY, READREPLY};
		switch (msg.type)
		{
		case MessageType::READ:
		{
			const auto &key = msg.key;
			const auto value = readKey(key);
			std::cout << "MessageType::READ on " << memberNode->addr.getAddress()
					  << " from " << msg.fromAddr.getAddress()
					  << " " << key << ", " << value << "\n";
			if (not value.empty())
			{
				log->logReadSuccess(&(memberNode->addr), false /*is_coordinator*/, msg.transID, key, value);
				auto reply = Message(msg.transID, memberNode->addr, value);
				// std::cout << "reply.toString(): " << reply.toString() << "\n";
				emulNet->ENsend(&(memberNode->addr), &(msg.fromAddr), reply.toString());
			}
			else
			{
				log->logReadFail(&(memberNode->addr), false /*is_coordinator*/, msg.transID, key);
			}
			break;
		}
		case MessageType::UPDATE:
		{
			const auto &key = msg.key;
			const auto &value = msg.value;
			const auto &replica = msg.replica;
			if (updateKeyValue(key, value, replica))
			{
				if (msg.transID >= 0)
				{
					log->logUpdateSuccess(&(memberNode->addr), false /*is_coordinator*/, msg.transID, key, value);
					auto reply = Message(msg.transID, memberNode->addr, MessageType::REPLY, true);
					emulNet->ENsend(&(memberNode->addr), &(msg.fromAddr), reply.toString());
				}
			}
			else
			{
				log->logUpdateFail(&(memberNode->addr), false /*is_coordinator*/, msg.transID, key, value);
			}
			break;
		}
		case MessageType::CREATE:
		{
			const auto &key = msg.key;
			const auto &value = msg.value;
			const auto &replica = msg.replica;
			// std::cout << "MessageType::CREATE/UPDATE on " << memberNode->addr.getAddress()
			// 		  << " from " << msg.fromAddr.getAddress() << " key: "
			// 		  << key << "," << msg.replica << "\n";
			createKeyValue(key, value, replica);
			if (msg.transID >= 0)
			{
				log->logCreateSuccess(&(memberNode->addr), false /*is_coordinator*/, msg.transID, key, value);
				auto reply = Message(msg.transID, memberNode->addr, MessageType::REPLY, true);
				emulNet->ENsend(&(memberNode->addr), &(msg.fromAddr), reply.toString());
			}
			break;
		}
		case MessageType::DELETE:
		{
			const auto &key = msg.key;
			if (deletekey(key))
			{
				log->logDeleteSuccess(&(memberNode->addr), false /*is_coordinator*/, msg.transID, key);
				auto reply = Message(msg.transID, memberNode->addr, MessageType::REPLY, true);
				emulNet->ENsend(&(memberNode->addr), &(msg.fromAddr), reply.toString());
			}
			else
			{
				log->logDeleteFail(&(memberNode->addr), false /*is_coordinator*/, msg.transID, key);
			}
			break;
		}
		case MessageType::READREPLY:
		{
			auto it = local_state_.find(msg.transID);
			// for (auto &v : local_state_)
			// {
			// 	std::cout << "v: " << v.first << "???" << v.second.key << "\n";
			// }
			if (it != local_state_.end())
			{
				const auto &key = it->second.key;
				auto &value = it->second.value;
				auto &replies = it->second.replies;
				std::cout << "MessageType::READREPLY on " << memberNode->addr.getAddress()
						  << " from " << msg.fromAddr.getAddress() << " key: "
						  << key << "," << msg.value << "\n";
				if (value.empty())
				{
					value = msg.value;
				}
				++replies;
				if (replies >= READ_QUORUM)
				{
					// READ success
					log->logReadSuccess(&(memberNode->addr), true /*is_coordinator*/, msg.transID, key, value);
					local_state_.erase(it);
				}
			}
			break;
		}
		case MessageType::REPLY:
		{
			auto it = local_state_.find(msg.transID);
			if (it != local_state_.end())
			{
				auto &state = it->second;
				const auto &key = state.key;
				const auto &value = state.value;
				auto &replies = state.replies;
				std::cout << "MessageType::REPLY for " << memberNode->addr.getAddress()
						  << " from " << msg.fromAddr.getAddress() << " key: "
						  << key << "," << value << "," << msg.type << "\n";
				++replies;
				if (replies >= WRITE_QUORUM)
				{
					// READ success
					// std::cout << "log->logCreateSuccess\n";
					if (state.type == MessageType::CREATE)
					{
						log->logCreateSuccess(&(memberNode->addr), true /*is_coordinator*/, msg.transID, key, value);
					}
					else if (state.type == MessageType::UPDATE)
					{
						log->logUpdateSuccess(&(memberNode->addr), true /*is_coordinator*/, msg.transID, key, value);
					}
					else if (state.type == MessageType::DELETE)
					{
						log->logDeleteSuccess(&(memberNode->addr), true /*is_coordinator*/, msg.transID, key);
					}
					local_state_.erase(it);
				}
			}

			break;
		}

		default:
			break;
		}
	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key)
{
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3)
	{
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size() - 1).getHashCode())
		{
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else
		{
			// go through the ring until pos <= node
			for (int i = 1; i < ring.size(); i++)
			{
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode())
				{
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i + 1) % ring.size()));
					addr_vec.emplace_back(ring.at((i + 2) % ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop()
{
	deleteExpired();
	if (memberNode->bFailed)
	{
		return false;
	}
	else
	{
		return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
	}
}

void MP2Node::deleteExpired()
{
	if (par->getcurrtime() > local_time)
	{
		local_time = par->getcurrtime();
		for (auto it = local_state_.begin(); it != local_state_.end();)
		{
			int trans_id = it->first;
			const auto &state = it->second;
			if (state.expiry < local_time)
			{
				switch (state.type)
				{
				case MessageType::READ:
				{
					log->logReadFail(&(memberNode->addr), true /*is_coordinator*/, trans_id, state.key);
					break;
				}
				case MessageType::CREATE:
				{
					log->logCreateFail(&(memberNode->addr), true /*is_coordinator*/, trans_id, state.key, state.value);
					break;
				}
				case MessageType::UPDATE:
				{
					log->logUpdateFail(&(memberNode->addr), true /*is_coordinator*/, trans_id, state.key, state.value);
					break;
				}
				case MessageType::DELETE:
				{
					log->logDeleteFail(&(memberNode->addr), true /*is_coordinator*/, trans_id, state.key);
					break;
				}
				default:
					break;
				}
				it = local_state_.erase(it);
			}
			else
			{
				++it;
			}
		}
	}
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size)
{
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol()
{
	PrintRing(ring);
	std::cout << "Rehashing primary " << ht->currentSize() << "\n";
	for (auto it = ht->hashTable.begin(); it != ht->hashTable.end();)
	{
		const auto &key = it->first;
		const auto &value = it->second;
		int current = g_transID++;
		auto nodes = findNodes(key);
		bool belongs_here = false;
		for (auto node_id = 0; node_id < nodes.size(); ++node_id)
		{
			if (node_id == 0 and *(nodes[node_id].getAddress()) == memberNode->addr)
			{
				belongs_here = true;
			}
			else
			{
				auto req = Message(-current, memberNode->addr, MessageType::CREATE, key, value,
								   static_cast<ReplicaType>(node_id));
				emulNet->ENsend(&(memberNode->addr), nodes[node_id].getAddress(), req.toString());
				// local_state_[current] = {MessageType::CREATE, key, value, par->getcurrtime() + WRITE_TIMEOUT, 0};
			}
		}
		if (belongs_here)
		{
			++it;
		}
		else
		{
			it = ht->hashTable.erase(it);
		}
	}
	std::cout << "Rehashing secondary " << secondary->currentSize() << "\n";
	for (auto it = secondary->hashTable.begin(); it != secondary->hashTable.end();)
	{
		const auto &key = it->first;
		const auto &value = it->second;
		int current = g_transID++;
		auto nodes = findNodes(key);
		bool belongs_here = false;
		for (auto node_id = 0; node_id < nodes.size(); ++node_id)
		{
			if (node_id == 1 and *(nodes[node_id].getAddress()) == memberNode->addr)
			{
				belongs_here = true;
			}
			else
			{
				auto req = Message(-current, memberNode->addr, MessageType::CREATE, key, value,
								   static_cast<ReplicaType>(node_id));
				emulNet->ENsend(&(memberNode->addr), nodes[node_id].getAddress(), req.toString());
			}
		}
		if (belongs_here)
		{
			++it;
		}
		else
		{
			it = secondary->hashTable.erase(it);
		}
	}
	std::cout << "Rehashing tertiary " << tertiary->currentSize() << "\n";
	for (auto it = tertiary->hashTable.begin(); it != tertiary->hashTable.end();)
	{
		const auto &key = it->first;
		const auto &value = it->second;
		int current = g_transID++;
		auto nodes = findNodes(key);
		bool belongs_here = false;
		for (auto node_id = 0; node_id < nodes.size(); ++node_id)
		{
			if (node_id == 2 and *(nodes[node_id].getAddress()) == memberNode->addr)
			{
				belongs_here = true;
			}
			else
			{
				auto req = Message(-current, memberNode->addr, MessageType::CREATE, key, value,
								   static_cast<ReplicaType>(node_id));
				emulNet->ENsend(&(memberNode->addr), nodes[node_id].getAddress(), req.toString());
			}
		}
		if (belongs_here)
		{
			++it;
		}
		else
		{
			it = tertiary->hashTable.erase(it);
		}
	}
}
