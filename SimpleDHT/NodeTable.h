#pragma once


#include <string>
#include <unordered_map>
#include <map>
#include <unordered_set>
#include <mutex>
#include "Utils.h"

struct Node
{
public:
	Node(size_t c_tag, size_t machine_id) : tag(c_tag), m_id(machine_id)
	{
		
	}
	Node()
	{
		tag = 0;
		m_id = 0;
	}

	size_t tag;
	size_t m_id;
};

class NodeTable
{
public:

	//adds virtual node
	void add_node(Node node)
	{
		std::lock_guard<std::mutex> guard(table_mutex);
		virtual_nodes[node.tag] = node;
		if (real_nodes.find(node.m_id) == real_nodes.end())
			real_nodes[node.m_id] = std::unordered_set<size_t>();
		real_nodes[node.m_id].insert(node.tag);
	}
	//remove real node
	void remove_machine(size_t id)
	{
		std::lock_guard<std::mutex> guard(table_mutex);
		if (real_nodes.find(id) != real_nodes.end())
		{
			for (auto tag : real_nodes[id])
			{
				virtual_nodes.erase(tag);
			}
			real_nodes.erase(id);
		}
	}

	//get node holding a tag
	Node get_node(size_t tag)
	{
		std::lock_guard<std::mutex> guard(table_mutex);
		auto it = virtual_nodes.upper_bound(tag);
		if (it == virtual_nodes.begin())
		{
			return (--virtual_nodes.end())->second;
		}
		return (--it)->second;
	}

private:
	std::map<size_t, Node> virtual_nodes;
	std::unordered_map<size_t, std::unordered_set<size_t>> real_nodes;
	std::mutex table_mutex;
};
