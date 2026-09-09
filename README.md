# vk_prac
Venue King practice

# Language
Go 1.26.3 linux/amd64

# Run
Go and Python are required to be installed.
```bash
./run.sh
```

# Assumptions
- source-a fetch function doesn't require Retry After and Rate Limiting handling routine.
- source-b fetch function doesn't require Rate Limiting handling routine.
- Only source-c requires both Retry After and Rate Limiting handling routine.

# Intentionally Omitted
- Testing code isn't implemented due to 4 hour time limit.
