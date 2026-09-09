# Goal
- Implementation of customized fetch functions for each source according to their requirements/behavior.
- Not trying to write an abstract(generalized) fetch function that can be used or cover all failure behaviors from each source.
- Concurrently fetching from all 3 sources to minimize running time.
- Final result contains a single array(one slice base pointer) of all the aggregate data rather than having 3 separate arrays, although 3 separate arrays could have been memory usage and running time even further because they can be used like a "lock-free queue" for each source as explained in the main function with the comments.

# Failure behavior
- Each Http request has 5 seconds time-out and each source has 7 max try regardless of what the source of error is.
- When max try is exceeded, the fetching loop for that source is terminated. Data collected upto that point have been already streamed concurrently while fetching.

# Assumption and Ambiguity
- Termination of the fetching loop source-B and source-C were determined by re-visitation of the previous fetched cursor and offset, respectively.
- The initial starting cursor and offset were arbitrarily set to empty string("") and 0.
- The limit(max page size) and Rate Limit values can be dynamically changed while fetching, so they were checked and updated for every loop. 

# Testing
- Testing code isn't implemented due to 4 hour time limit.
