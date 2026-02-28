---
globs: "**/*.md"
alwaysApply: true
---
# Atomic Plan Tasks Rule

## Rule

When creating an implementation plan, each task must be as small and atomic as possible.

- A task does exactly **one thing** with a single, clear verification step.
- If a task involves more than one distinct artifact, split it.
- Prefer many small tasks over fewer large ones — granularity aids context management and reduces rework.
- Each task description should be completable without ambiguity about scope.

## What counts as a separate task

Each of the following is its own task — never bundle them together:

- Each **pydantic model** or **dataclass**
- Each **abstract class** (Protocol / ABC)
- Each **concrete implementation** of an abstract class
- Each **service function** or method
- Each **API endpoint**
- Each **database table / migration**
- Each **configuration class** (Settings, env file, docker-compose entry)
- Each **test class or test file**
- Each **test fixture or factory**
- Each **wiring / integration step** (registering a router, adding a dependency, connecting components)
- Each **documentation artifact** (README, docstring update)

## Example

Bad:
```
- [ ] Step 1: Create the User and Order pydantic models
- [ ] Step 2: Create the BaseRepository ABC and PostgresRepository implementation
- [ ] Step 3: Create the POST and GET /users endpoints
- [ ] Step 4: Write tests and update README
```

Good:
```
- [ ] Step 1: Create the User pydantic model
- [ ] Step 2: Create the Order pydantic model
- [ ] Step 3: Create the BaseRepository abstract class
- [ ] Step 4: Create the PostgresRepository concrete implementation
- [ ] Step 5: Create the POST /users endpoint
- [ ] Step 6: Create the GET /users endpoint
- [ ] Step 7: Register the users router in main.py
- [ ] Step 8: Write tests for the POST /users endpoint
- [ ] Step 9: Write tests for the GET /users endpoint
- [ ] Step 10: Create the service README
```
