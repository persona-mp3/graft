## Current state

### Leader Election
All nodes in a cluster can come to an election, however there are still things that need addressing
1. If a node was a leader in a prev term, and got killed. What are the odds, that it doesn't start campaigning 
again because it's hearbeat timeout triggered way ealier than the other clusters, while a leader is still active. 
Well the answer here is *Term* as mentioned in the Raft Paper

So the way this is going to be impl, is that for each election, there will be a term.
If the resurrected node tries campaigin, other nodes will check against their terms. 


#### Problem
I have to do some protocol wiring


