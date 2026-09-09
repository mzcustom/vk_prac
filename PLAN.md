# Major Technical Decision
- Products/Items/Data array fields were defined using a generic type, because otherwise, the deserialization error of wrongly typed row can make the entire page of data thrown even if other rows are well formatted.
- Each fetch function was written strictly following the expected behavior of the mock API explained in the task.md. For instance, source-a was assumed to be not require Retry After and Rate Limiting handling routines.
- Each fetch function was not further abstracted using type tag/switch statement or interface to avoid OOP like complexities.
- Instead of giving an unshared array(slice) to the worker goroutine for each source, and use them as a separate lock-free queue, Fan-In pattern with a channel was used so that the result will have a single array base pointer that contains data from all 3 sources.
- Instead of global time-out for entire fetching process, 7 max tries were given to each source so the program doesn't get stuck in an infinite loop when the server has an unexpected issue. In Golang, the global time-out is usually controlled by the "context", but the "Max try" method was chosen because the code is more universally understood just by reading it without knowing any Go specific device like "context".  

# Testing and Verification
- Testing code isn't implemented due to the 4 hour time limit.
