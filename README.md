weimar-graphstore
=================

'Weimar' denotes a graph abstraction layer which leverages the distributed key-value store [HyperDex](https://github.com/rescrv/HyperDex/)
(version 1.1) as underlying storage back-end. Main objective is to create a database for massive parallel access to a
rapid changing graph. Background of this work is a student research project at the 'University of Cooperative Education' (DHBW Mannheim).
**This is still prototypic work and subject to development.**

# The Graph Model
The supported graph is an advanced property graph model with nodes (vertices) and links (edges) connecting these
components. Graphs can represent entities and relationships between entities, and in addition to the link structure,
information can be associated with both links and nodes of the graph.
Weimar provides a type system for both nodes and links, which means, every single graph element has a corresponding
type. A further improvement over simple graph models is, links of the same entity can point to multiple target 
nodes. In terms of storage this is very efficient, since Weimar needs to retrieve such links merely once in order to
traverse the graph accordingly.
The chosen graph model is very well supported by the concepts of HyperDex. Each VertexType/EdgeType object maps to
its own Hyperspace (in HyperDex), hence nodes and links are distributed throughout multiple Hyperspaces.
There is a paper coming soon, describing the graph model in more detail. 

# Features
The following features are already implemented:
* Type system: create VertexType and EdgeType objects and a corresponding container with a type definition (similar to tables in RDBMS)
* Storing vertices/edges: insert vertices and edges, i.e. create a graph structure by connecting vertices via edges
* Storing properties: structured and unstructured attributes can be associated with vertices and edges
* Search: search vertices based on attribute values

The following features are currently planned:
* Search: search for edges, graph pattern 
* Server: dedicated/shared graph server
* Algorithms: a lib for multiple build-in graph algorithms
* Performance: the code is supposed to be compiled with Cython, also multiprocessing will be used

# How to use
The following code is taken from the unittest tests/testhyperdexgraph.py. It shows the usage of the
HyperDexGraph API for a small movie database. Firstly, the following code requires a running 
[HyperDex](https://github.com/rescrv/HyperDex/). Please refer to their documentation on
http://hyperdex.org/ how to set up the database. Once a HyperDex instance is running, to following
code can be executed:

```

from graph.hyperdexgraph import HyperDexGraph
from graph.requestgraphelements import RequestVertexType, RequestEdgeType, RequestVertex

#open the graph
GRAPH_NAME = 'default'
g=HyperDexGraph('127.0.0.1', 1990, GRAPH_NAME)

#create vertex type User with structured attributes
user_type=RequestVertexType('User', ('string', 'first'), ('string', 'last'), ('int', 'age'))
user=g.create_vertex_type(user_type)
#create vertex type Movie with structured attributes
movie_type=RequestVertexType('Movie', ('string', 'title'), ('int', 'length'), ('int', 'released'), ('string', 'type'))
movie=g.create_vertex_type(movie_type)
#create edge type rates with a structured attribute
rates_type=RequestEdgeType('rates', ('int', 'stars'))
rates=g.create_edge_type(rates_type)
#insert some users, fill in the data
u1=g.insert_vertex(RequestVertex(user, {'first':'Scrooge', 'last':'McDuck', 'age':67}))
u2=g.insert_vertex(RequestVertex(user, {'first':'Donald', 'last':'Duck', 'age':77}))
u3=g.insert_vertex(RequestVertex(user, {'first':'Mikey', 'last':'Mouse', 'age':80}))
u4=g.insert_vertex(RequestVertex(user, {'first':'Gus', 'last':'Goose', 'age':76}))
u5=g.insert_vertex(RequestVertex(user, {'first':'Black', 'last':'Pete', 'age':83}))
#insert some movies, fill in the required data
m1=g.insert_vertex(RequestVertex(movie, {'title':'DuckTails', 'length':22, 'released':1987,'type':'series'}))
m2=g.insert_vertex(RequestVertex(movie, {'title':'The Wise Little Hen', 'length':7, 'released':1934,'type':'film'}))
m3=g.insert_vertex(RequestVertex(movie, {'title':'Mickey\'s Christmas Carol', 'length':25, 'released':1983,'type':'film'}))
m4=g.insert_vertex(RequestVertex(movie, {'title':'Darkwing Duck', 'length':22, 'released':1991,'type':'series'}))
#attach some movie ratings with the number of stars given
u1.add_edge(m1, rates, {'stars':5})
u2.add_edge(m1, rates, {'stars':3})
u2.add_edge(m2, rates, {'stars':1})
u2.add_edge(m4, rates, {'stars':5})
u3.add_edge(m2, rates, {'stars':4})
u3.add_edge(m3, rates, {'stars':5})
u3.add_edge(m1, rates, {'stars':2})
u4.add_edge(m1, rates, {'stars':4})
u4.add_edge(m4, rates, {'stars':5})
u5.add_edge(m1, rates, {'stars':1})
u5.add_edge(m3, rates, {'stars':1})
u5.add_edge(m4, rates, {'stars':2})
#perform a simple traversal, following only the 'rates' type of edge
self.assertEqual('DuckTails',u1.get_outgoing_edges(rates)[0].get_target()[0].get_property('title'))
#calc avg rating of DuckTails (m1)
r, c = 0,0
for rating in m1.get_incoming_edges(rates):
  c = c + 1
  r = r + rating.get_property('stars') # Average stars will be 15 / 5 = 3
#search vertices
l1=g.search_vertex(user, ('last', 'Duck'))
r = []
for vertex in l1:
  print(vertex.get_property('first') + ' ' + vertex.get_property('last'))
        
#Reqex search is not yet implemented

#remove an item recursively 
m4.remove()
#remove yet another item 
u1.remove()
#calc avg rating of DuckTails again
r, c = 0,0
for rating in m1.get_incoming_edges(rates):
   c = c + 1
   r = r + rating.get_property('stars') # Average stars will be now 10 / 4 = 2.5
        
#add some unstructured attributes
m1.set_property('comment', 'DuckTales is an American animated television series produced by Disney Television Animation.')

#add structured/unstructured attributes - the other way (this is also persistent)
m2.comment = 'The Wise Little Hen is a Walt Disney\'s Silly Symphonies cartoon, based on the fairy tale The Little Red Hen'



#remove everthing
user.remove()
movie.remove()
rates.remove()

```



